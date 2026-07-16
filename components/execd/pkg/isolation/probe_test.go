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
	"errors"
	"slices"
	"testing"
)

func TestParseBwrapVersion(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"bubblewrap 0.8.0\n", "0.8.0"},
		{"bwrap 0.10.0\n", "0.10.0"},
		{"bwrap: unrecognized option '--version'\n", ""},
		{"", ""},
		{"some unrelated output\nbubblewrap 0.11.2-dev\nmore output", "0.11.2"},
	}

	for _, tt := range tests {
		got := parseBwrapVersion(tt.in)
		if got != tt.want {
			t.Errorf("parseBwrapVersion(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestProbeConfigDefaults(t *testing.T) {
	cfg := ProbeConfig{
		UpperRoot:     "/var/lib/execd/isolation",
		UpperMaxBytes: 8 * 1024 * 1024 * 1024,
	}
	if cfg.UpperRoot == "" {
		t.Error("UpperRoot should not be empty")
	}
}

func TestProbeResult_Defaults(t *testing.T) {
	result := ProbeResult{}
	if result.Available {
		t.Error("default ProbeResult should have Available=false")
	}
	if result.CommitSupported {
		t.Error("default ProbeResult should have CommitSupported=false")
	}
	if result.DiffSupported {
		t.Error("default ProbeResult should have DiffSupported=false")
	}
	if result.SetprivAvailable || result.UsernsAvailable {
		t.Error("default ProbeResult should have both uid modes unavailable")
	}
}

func TestSetBwrapModeAvailability(t *testing.T) {
	probeErr := errors.New("probe failed")
	tests := []struct {
		name          string
		setprivErr    error
		usernsErr     error
		wantAvailable bool
		wantSetpriv   bool
		wantUserns    bool
	}{
		{name: "both available", wantAvailable: true, wantSetpriv: true, wantUserns: true},
		{name: "setpriv only", usernsErr: probeErr, wantAvailable: true, wantSetpriv: true},
		{name: "userns only", setprivErr: probeErr, wantAvailable: true, wantUserns: true},
		{name: "neither available", setprivErr: probeErr, usernsErr: probeErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ProbeResult{}
			setBwrapModeAvailability(&result, tt.setprivErr, tt.usernsErr)
			if result.Available != tt.wantAvailable {
				t.Errorf("Available = %v, want %v", result.Available, tt.wantAvailable)
			}
			if result.SetprivAvailable != tt.wantSetpriv {
				t.Errorf("SetprivAvailable = %v, want %v", result.SetprivAvailable, tt.wantSetpriv)
			}
			if result.UsernsAvailable != tt.wantUserns {
				t.Errorf("UsernsAvailable = %v, want %v", result.UsernsAvailable, tt.wantUserns)
			}
		})
	}
}

func TestBwrapSmokeArgs_UsernsFlag(t *testing.T) {
	setprivArgs := bwrapSmokeArgs(false)
	if slices.Contains(setprivArgs, "--unshare-user") {
		t.Errorf("setpriv smoke args unexpectedly contain --unshare-user: %v", setprivArgs)
	}

	usernsArgs := bwrapSmokeArgs(true)
	if !slices.Contains(usernsArgs, "--unshare-user") {
		t.Errorf("userns smoke args do not contain --unshare-user: %v", usernsArgs)
	}
}
