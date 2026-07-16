// Copyright 2026 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package isolation

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/alibaba/opensandbox/execd/pkg/log"
)

// ProbeResult holds the result of startup isolation probing.
type ProbeResult struct {
	Available        bool
	Isolator         string
	Version          string
	Message          string // diagnostic message when unavailable
	SetprivAvailable bool   // setpriv uid mode can create the required namespaces
	UsernsAvailable  bool   // userns uid mode can create the required namespaces
	CommitSupported  bool   // Phase 2
	DiffSupported    bool   // Phase 2
	PersistAvailable bool   // Phase 2 — requires emptyDir
}

// ProbeConfig controls Probe behaviour.
type ProbeConfig struct {
	UpperRoot     string
	UpperMaxBytes int64
}

// Probe runs startup detection. Returns a ProbeResult describing what
// isolation capabilities are available in the current environment.
//
// On Linux with working bwrap:
//
//	Available=true, Isolator="bwrap", Version="0.10.0"
//
// Otherwise:
//
//	Available=false
func Probe(cfg ProbeConfig) ProbeResult {
	result := ProbeResult{}

	// Check if bwrap binary is available.
	version, err := probeBwrapVersion()
	if err != nil {
		result.Message = fmt.Sprintf("bwrap not found: %v (searched: $PATH, /opt/opensandbox/bwrap, /usr/bin/bwrap, /usr/local/bin/bwrap)", err)
		log.Warn("isolation probe: %s", result.Message)
		return result
	}

	result.Isolator = "bwrap"
	result.Version = version

	// Probe each uid mode independently. Some environments allow an
	// unprivileged user namespace but do not grant the capabilities required
	// by setpriv mode (or vice versa), so one failing mode must not disable the
	// other.
	setprivErr := probeBwrapSetprivSmoke()
	usernsErr := probeBwrapUsernsSmoke()
	setBwrapModeAvailability(&result, setprivErr, usernsErr)
	if setprivErr != nil {
		log.Warn("isolation probe: setpriv uid mode unavailable: %v", setprivErr)
	}
	if usernsErr != nil {
		log.Warn("isolation probe: userns uid mode unavailable: %v", usernsErr)
	}
	if !result.Available {
		result.Message = fmt.Sprintf(
			"bwrap found (v%s) but no uid mode is available (setpriv: %v; userns: %v)",
			version, setprivErr, usernsErr,
		)
		log.Warn("isolation probe: %s", result.Message)
		return result
	}

	if probeOverlayMount(cfg.UpperRoot) {
		result.CommitSupported = true
		result.DiffSupported = true
	}

	return result
}

func setBwrapModeAvailability(result *ProbeResult, setprivErr, usernsErr error) {
	result.SetprivAvailable = setprivErr == nil
	result.UsernsAvailable = usernsErr == nil
	result.Available = result.SetprivAvailable || result.UsernsAvailable
}

// probeBwrapVersion returns the bwrap version string if available.
func probeBwrapVersion() (string, error) {
	p := findBwrap()
	if p == "" {
		return "", fmt.Errorf("bwrap not found")
	}

	var stdout bytes.Buffer
	cmd := exec.Command(p, "--version")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	// bwrap prints version to stdout, e.g.:
	// "bubblewrap 0.8.0" or "bwrap 0.10.0"
	out := stdout.String()
	return parseBwrapVersion(out), nil
}

var bwrapVersionRe = regexp.MustCompile(`b(?:ubble)?wrap\s+(\d+\.\d+\.\d+)`)

// parseBwrapVersion extracts the version number from bwrap --version output.
func parseBwrapVersion(out string) string {
	match := bwrapVersionRe.FindStringSubmatch(out)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

// probeBwrapSetprivSmoke verifies bwrap can create the namespaces used by the
// default setpriv uid mode.
func probeBwrapSetprivSmoke() error {
	return probeBwrapSmoke(false)
}

// probeBwrapUsernsSmoke verifies bwrap can create the namespaces used by the
// userns uid mode.
func probeBwrapUsernsSmoke() error {
	return probeBwrapSmoke(true)
}

func probeBwrapSmoke(userns bool) error {
	p := findBwrap()
	if p == "" {
		return fmt.Errorf("bwrap not found")
	}
	cmd := exec.Command(p, bwrapSmokeArgs(userns)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bwrap smoke test failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func bwrapSmokeArgs(userns bool) []string {
	args := []string{
		"--unshare-pid", "--unshare-uts", "--unshare-ipc", "--unshare-cgroup",
		"--ro-bind", "/", "/",
		"--proc", "/proc",
		"--", "true",
	}
	if userns {
		args = append([]string{"--unshare-user"}, args...)
	}
	return args
}

// probeOverlayMount tests whether bwrap can create an overlay mount.
func probeOverlayMount(upperRoot string) bool {
	p := findBwrap()
	if p == "" {
		return false
	}

	// Probe on the upper root filesystem (typically tmpfs/emptyDir) rather
	// than /tmp, because overlayfs cannot nest on Docker's overlay2 layer
	// but works fine on tmpfs.
	base := upperRoot
	if base == "" {
		base = os.TempDir()
	}
	tmpDir, err := os.MkdirTemp(base, "execd-probe-overlay-*")
	if err != nil {
		log.Warn("isolation probe: overlay: MkdirTemp(%s): %v", base, err)
		return false
	}
	defer os.RemoveAll(tmpDir)

	lowerDir := filepath.Join(tmpDir, "lower")
	upperDir := filepath.Join(tmpDir, "upper")
	workDir := filepath.Join(tmpDir, "work")
	for _, d := range []string{lowerDir, upperDir, workDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Warn("isolation probe: overlay: MkdirAll(%s): %v", d, err)
			return false
		}
	}

	cmd := exec.Command(p,
		"--ro-bind", "/", "/",
		"--proc", "/proc",
		"--overlay-src", lowerDir,
		"--overlay", upperDir, workDir, "/mnt",
		"--", "true",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Warn("isolation probe: overlay mount failed: %v (stderr: %s)", err, strings.TrimSpace(stderr.String()))
		return false
	}
	return true
}
