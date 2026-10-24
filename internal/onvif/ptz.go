package onvif

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/bc"
	"github.com/AlexxIT/go2rtc/internal/streams"
	pkgbc "github.com/AlexxIT/go2rtc/pkg/bc"
	"github.com/AlexxIT/go2rtc/pkg/onvif"
)

// profilesWait is how long GetProfiles waits for cameras whose abilities are not
// known yet, those that did not answer have no PTZ in this answer and get it in
// the next one.
var profilesWait = 8 * time.Second

// maxPresets is what is claimed in the PTZ node, 0..63 are the ids go2rtc uses
// for a free slot.
const maxPresets = 64

// ptzController is what the handler needs of a camera, *bc.PTZ is one.
type ptzController interface {
	Caps(ctx context.Context) (pkgbc.PTZCaps, error)
	Move(v pkgbc.Velocity, timeout time.Duration) error
	Stop(panTilt, zoom bool) error
	Presets() ([]pkgbc.Preset, error)
	GotoPreset(id uint8) error
	SetPreset(id int, name string) (uint8, error)
}

// ptzFor is the controller of a camera by its bc:// URL, tests replace it.
var ptzFor = func(rawURL string) (ptzController, error) {
	return pkgbc.PTZFor(rawURL)
}

// bcSource is the first source of the stream that is a bc:// one, "" if none.
func bcSource(name string) string {
	return bc.Source(name)
}

// hasBCStreams tells if there is a bc:// stream at all, the device then offers
// the PTZ service.
func hasBCStreams() bool {
	for _, name := range streams.GetAllNames() {
		if bcSource(name) != "" {
			return true
		}
	}
	return false
}

// ptzFeatures asks the cameras of the bc:// streams in names, all together
// within wait. The cameras that did not answer in time are left out, their
// request goes on and fills the cache.
func ptzFeatures(names []string, wait time.Duration) map[string]onvif.PTZFeatures {
	var mu sync.Mutex
	result := map[string]onvif.PTZFeatures{}
	forCamera := ptzFor // read now, the goroutines may outlive the call

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	var wg sync.WaitGroup
	for _, name := range names {
		source := bcSource(name)
		if source == "" {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			c, err := forCamera(source)
			if err != nil {
				log.Debug().Err(err).Msgf("[onvif] ptz stream=%s", name)
				return
			}
			caps, err := c.Caps(ctx)
			if err != nil {
				log.Debug().Err(err).Msgf("[onvif] ptz caps stream=%s", name)
				return
			}
			if caps.PanTilt || caps.Zoom {
				mu.Lock()
				result[name] = onvif.PTZFeatures{PanTilt: caps.PanTilt, Zoom: caps.Zoom}
				mu.Unlock()
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}

	mu.Lock()
	defer mu.Unlock()
	out := make(map[string]onvif.PTZFeatures, len(result))
	for k, v := range result {
		out[k] = v
	}
	return out
}

func writeSOAP(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/soap+xml; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(b); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func fault(w http.ResponseWriter, subcode, reason string) {
	log.Debug().Msgf("[onvif] ptz fault %s: %s", subcode, reason)
	writeSOAP(w, http.StatusInternalServerError, onvif.FaultResponse(subcode, reason))
}

// camera is the controller of the camera behind the stream with this profile token.
func camera(token string) (ptzController, bool) {
	source := bcSource(token)
	if source == "" {
		return nil, false
	}
	c, err := ptzFor(source)
	if err != nil {
		log.Debug().Err(err).Msgf("[onvif] ptz stream=%s", token)
		return nil, false
	}
	return c, true
}

// allFeatures lists the streams with a camera that can move, by name.
func allFeatures() []onvif.PTZNamed {
	names := streams.GetAllNames()
	sort.Strings(names)
	features := ptzFeatures(names, profilesWait)

	var list []onvif.PTZNamed
	for _, name := range names {
		if f, ok := features[name]; ok {
			list = append(list, onvif.PTZNamed{Name: name, PTZFeatures: f})
		}
	}
	return list
}

// featuresOf is the features behind a token like ptz_<stream>, with prefix removed.
func featuresOf(token, prefix string) (name string, f onvif.PTZFeatures, ok bool) {
	if !strings.HasPrefix(token, prefix) {
		return "", f, false
	}
	name = token[len(prefix):]
	f, ok = ptzFeatures([]string{name}, profilesWait)[name]
	return name, f, ok
}

func presetID(token string) (uint8, bool) {
	n, err := strconv.ParseUint(token, 10, 8)
	return uint8(n), err == nil
}

func ptzService(w http.ResponseWriter, r *http.Request, operation string, b []byte) {
	if operation == "" {
		fault(w, "InvalidArgVal", "malformed request body")
		return
	}

	switch operation {
	case onvif.ServiceGetServiceCapabilities:
		writeSOAP(w, http.StatusOK, onvif.PTZServiceCapabilitiesResponse())

	case onvif.PTZGetConfigurations:
		writeSOAP(w, http.StatusOK, onvif.PTZConfigurationsResponseAll(allFeatures()))

	case onvif.PTZGetConfiguration:
		token := onvif.FindTagValue(b, "PTZConfigurationToken")
		name, f, ok := featuresOf(token, "ptz_")
		if !ok {
			fault(w, "NoConfig", "no such PTZ configuration")
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZConfigurationResponse(name, f))

	case onvif.PTZGetConfigurationOptions:
		token := onvif.FindTagValue(b, "ConfigurationToken")
		_, f, ok := featuresOf(token, "ptz_")
		if !ok {
			fault(w, "NoConfig", "no such PTZ configuration")
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZConfigurationOptionsResponse(f))

	case onvif.PTZGetNodes:
		writeSOAP(w, http.StatusOK, onvif.PTZNodesResponseAll(allFeatures(), maxPresets))

	case onvif.PTZGetNode:
		token := onvif.FindTagValue(b, "NodeToken")
		name, f, ok := featuresOf(token, "ptz_node_")
		if !ok {
			fault(w, "NoEntity", "no such PTZ node")
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZNodeResponse(name, f, maxPresets))

	case onvif.PTZContinuousMove:
		req, err := onvif.ParseContinuousMove(b)
		if err != nil {
			fault(w, "InvalidArgVal", err.Error())
			return
		}
		c, ok := camera(req.Token)
		if !ok {
			fault(w, "NoProfile", "no PTZ for this profile")
			return
		}
		v := pkgbc.Velocity{
			Pan: req.PanTilt.X, Tilt: req.PanTilt.Y, Zoom: req.Zoom,
			HasPanTilt: req.HasPanTilt, HasZoom: req.HasZoom,
		}
		if err = c.Move(v, req.Timeout); err != nil {
			fault(w, "Action", err.Error())
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZEmptyResponse(operation))

	case onvif.PTZStop:
		token, panTilt, zoom := onvif.ParseStop(b)
		c, ok := camera(token)
		if !ok {
			fault(w, "NoProfile", "no PTZ for this profile")
			return
		}
		if err := c.Stop(panTilt, zoom); err != nil {
			fault(w, "Action", err.Error())
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZEmptyResponse(operation))

	case onvif.PTZGetPresets:
		c, ok := camera(onvif.FindTagValue(b, "ProfileToken"))
		if !ok {
			fault(w, "NoProfile", "no PTZ for this profile")
			return
		}
		presets, err := c.Presets()
		if err != nil {
			fault(w, "Action", err.Error())
			return
		}
		list := make([]onvif.PTZPreset, 0, len(presets))
		for _, p := range presets {
			list = append(list, onvif.PTZPreset{Token: strconv.Itoa(int(p.ID)), Name: p.Name})
		}
		writeSOAP(w, http.StatusOK, onvif.PTZPresetsResponse(list))

	case onvif.PTZGotoPreset:
		token, presetToken, _ := onvif.ParsePreset(b)
		c, ok := camera(token)
		if !ok {
			fault(w, "NoProfile", "no PTZ for this profile")
			return
		}
		id, ok := presetID(presetToken)
		if !ok {
			fault(w, "NoToken", "no such preset")
			return
		}
		presets, err := c.Presets()
		if err != nil {
			fault(w, "Action", err.Error())
			return
		}
		known := false
		for _, p := range presets {
			known = known || p.ID == id
		}
		if !known {
			fault(w, "NoToken", "no such preset")
			return
		}
		if err = c.GotoPreset(id); err != nil {
			fault(w, "Action", err.Error())
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZEmptyResponse(operation))

	case onvif.PTZSetPreset:
		token, presetToken, name := onvif.ParsePreset(b)
		c, ok := camera(token)
		if !ok {
			fault(w, "NoProfile", "no PTZ for this profile")
			return
		}
		id := -1 // the lowest free slot
		if presetToken != "" {
			n, ok := presetID(presetToken)
			if !ok {
				fault(w, "NoToken", "no such preset")
				return
			}
			id = int(n)
		}
		n, err := c.SetPreset(id, name)
		if err != nil {
			fault(w, "Action", err.Error())
			return
		}
		writeSOAP(w, http.StatusOK, onvif.PTZSetPresetResponse(strconv.Itoa(int(n))))

	default:
		// RemovePreset, GetStatus, AbsoluteMove, RelativeMove...: not offered
		log.Warn().Msgf("[onvif] unsupported ptz operation: %s", operation)
		fault(w, "ActionNotSupported", "operation not supported")
	}
}
