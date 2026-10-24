package bc

import (
	"errors"
	"fmt"
)

// Direction is a PtzControl command.
type Direction string

const (
	DirLeft      Direction = "left"
	DirRight     Direction = "right"
	DirUp        Direction = "up"
	DirDown      Direction = "down"
	DirLeftUp    Direction = "leftUp"
	DirLeftDown  Direction = "leftDown"
	DirRightUp   Direction = "rightUp"
	DirRightDown Direction = "rightDown"
	DirStop      Direction = "stop"
)

// PtzControl is a move or stop request (msg 18).
type PtzControl struct {
	Version   string  `xml:"version,attr"`
	ChannelID uint8   `xml:"channelId"`
	Speed     float32 `xml:"speed"`
	Command   string  `xml:"command"`
}

// PtzPreset lists presets (msg 190 reply) or goes to / stores one (msg 19).
type PtzPreset struct {
	Version    string     `xml:"version,attr"`
	ChannelID  uint8      `xml:"channelId"`
	PresetList PresetList `xml:"presetList"`
}

type PresetList struct {
	Presets []Preset `xml:"preset"`
}

// Preset is a stored position in a list, or a command (toPos, setPos) in a request.
type Preset struct {
	ID      uint8  `xml:"id"`
	Name    string `xml:"name,omitempty"`
	Command string `xml:"command,omitempty"`
}

// PtzZoomFocus is the reply to msg 294, the zoom and focus positions.
type PtzZoomFocus struct {
	Version   string  `xml:"version,attr"`
	ChannelID uint8   `xml:"channelId"`
	Zoom      ZoomPos `xml:"zoom"`
	Focus     ZoomPos `xml:"focus"`
}

type ZoomPos struct {
	Max uint32 `xml:"maxPos"`
	Min uint32 `xml:"minPos"`
	Cur uint32 `xml:"curPos"`
}

// StartZoomFocus moves the lens (msg 295), command "zoomPos" goes to movePos.
type StartZoomFocus struct {
	Version   string `xml:"version,attr"`
	ChannelID uint8  `xml:"channelId"`
	Command   string `xml:"command"`
	MovePos   uint32 `xml:"movePos"`
}

// Support is the reply to msg 199, only the per channel PTZ type is mapped.
type Support struct {
	Version string        `xml:"version,attr,omitempty"`
	Items   []SupportItem `xml:"item"`
}

type SupportItem struct {
	ChnID   uint32  `xml:"chnID"`
	PtzType *uint32 `xml:"ptzType"`
}

// PTZCaps is what the camera can do for one channel.
type PTZCaps struct {
	PanTilt, Zoom    bool
	ZoomMin, ZoomMax uint32
}

// codeError is a camera that refused a request.
type codeError struct {
	id   uint32
	code uint16
}

func (e *codeError) Error() string {
	return fmt.Sprintf("bc: msg %d: camera answered %d", e.id, e.code)
}

// ptzCall is a request that is sent and whose reply is awaited separately.
type ptzCall struct {
	c  *Client
	sb *sub
	id uint32
}

// ptzStart sends a request. The channel Extension is left out for msg 199,
// which is asked of the device and sent header-only by neolink.
func (c *Client) ptzStart(id uint32, body *Body) (*ptzCall, error) {
	sb, err := c.subscribeNew(id)
	if err != nil {
		return nil, err
	}

	m := &Message{MsgID: id, ChannelID: c.cfg.Channel, MsgNum: sb.num, Class: ClassModern24, Body: body}
	if id != MsgGetSupport {
		m.Extension = NewExtension()
		m.Extension.ChannelID = &c.cfg.Channel
	}
	if err = c.send(m); err != nil {
		sb.close()
		return nil, err
	}
	return &ptzCall{c: c, sb: sb, id: id}, nil
}

// wait is the reply if the camera accepted the request. cancel ends the wait
// with ErrCanceled, the request may have been executed anyway.
func (k *ptzCall) wait(cancel <-chan struct{}) (*Message, error) {
	defer k.sb.close()
	r, err := k.sb.recvCancel(k.c.cfg.ReplyTimeout, cancel)
	if err != nil {
		if errors.Is(err, ErrCanceled) {
			return nil, err
		}
		return nil, fmt.Errorf("bc: msg %d: %w", k.id, err)
	}
	if r.ResponseCode != 200 {
		return nil, &codeError{k.id, r.ResponseCode}
	}
	return r, nil
}

func (c *Client) ptzRequest(id uint32, body *Body, cancel <-chan struct{}) (*Message, error) {
	k, err := c.ptzStart(id, body)
	if err != nil {
		return nil, err
	}
	return k.wait(cancel)
}

func (c *Client) ptzMoveBody(dir Direction, speed float32) *Body {
	return &Body{PtzControl: &PtzControl{
		Version: XMLVersion, ChannelID: c.cfg.Channel, Speed: speed, Command: string(dir),
	}}
}

// PTZMove starts a move in a direction, speed is 1..64, or stops with DirStop.
func (c *Client) PTZMove(dir Direction, speed float32) error {
	_, err := c.ptzRequest(MsgPTZControl, c.ptzMoveBody(dir, speed), nil)
	return err
}

// PTZPresets lists the stored presets.
func (c *Client) PTZPresets() ([]Preset, error) {
	r, err := c.ptzRequest(MsgGetPTZPreset, nil, nil)
	if err != nil {
		return nil, err
	}
	if r.Body == nil || r.Body.PtzPreset == nil {
		return nil, fmt.Errorf("bc: no PtzPreset in reply to msg %d", MsgGetPTZPreset)
	}
	return r.Body.PtzPreset.PresetList.Presets, nil
}

func (c *Client) presetBody(p Preset) *Body {
	return &Body{PtzPreset: &PtzPreset{
		Version: XMLVersion, ChannelID: c.cfg.Channel, PresetList: PresetList{Presets: []Preset{p}},
	}}
}

func (c *Client) ptzPreset(p Preset) error {
	_, err := c.ptzRequest(MsgPTZPreset, c.presetBody(p), nil)
	return err
}

// PTZGotoPreset moves the camera to a stored preset.
func (c *Client) PTZGotoPreset(id uint8) error {
	return c.ptzPreset(Preset{ID: id, Command: "toPos"})
}

// PTZSetPreset stores the current position as preset id.
func (c *Client) PTZSetPreset(id uint8, name string) error {
	return c.ptzPreset(Preset{ID: id, Name: name, Command: "setPos"})
}

// ZoomFocus reads the lens positions and the zoom range.
func (c *Client) ZoomFocus() (*PtzZoomFocus, error) { return c.zoomFocus(nil) }

func (c *Client) zoomFocus(cancel <-chan struct{}) (*PtzZoomFocus, error) {
	r, err := c.ptzRequest(MsgGetZoomFocus, nil, cancel)
	if err != nil {
		return nil, err
	}
	if r.Body == nil || r.Body.PtzZoomFocus == nil {
		return nil, fmt.Errorf("bc: no PtzZoomFocus in reply to msg %d", MsgGetZoomFocus)
	}
	return r.Body.PtzZoomFocus, nil
}

// ZoomTo moves the lens to an absolute position, the caller keeps it within
// the range from ZoomFocus.
func (c *Client) ZoomTo(pos uint32) error { return c.zoomTo(pos, nil) }

func (c *Client) zoomTo(pos uint32, cancel <-chan struct{}) error {
	_, err := c.ptzRequest(MsgSetZoomFocus, &Body{StartZoomFocus: &StartZoomFocus{
		Version: XMLVersion, ChannelID: c.cfg.Channel, Command: "zoomPos", MovePos: pos,
	}}, cancel)
	return err
}

// PTZCaps asks what the camera can move. A camera without a zoom lens refuses
// msg 294, that is Zoom=false and no error.
func (c *Client) PTZCaps() (PTZCaps, error) {
	var caps PTZCaps

	r, err := c.ptzRequest(MsgGetSupport, nil, nil)
	if err != nil {
		return caps, err
	}
	if r.Body != nil && r.Body.Support != nil {
		for _, it := range r.Body.Support.Items {
			if it.ChnID == uint32(c.cfg.Channel) && it.PtzType != nil && *it.PtzType != 0 {
				caps.PanTilt = true
			}
		}
	}

	zf, err := c.ZoomFocus()
	var ce *codeError
	if errors.As(err, &ce) {
		return caps, nil
	}
	if err != nil {
		return caps, err
	}
	if zf.Zoom.Max > zf.Zoom.Min {
		caps.Zoom = true
		caps.ZoomMin, caps.ZoomMax = zf.Zoom.Min, zf.Zoom.Max
	}
	return caps, nil
}
