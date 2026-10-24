package bc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPTZMoveSendsControl(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	require.NoError(t, c.PTZMove(DirLeftUp, 32))
	require.NoError(t, c.PTZMove(DirStop, 0))
	require.Equal(t, []string{"ptz:leftUp:32", "ptz:stop:0"}, f.events())
}

func TestPTZControlXML(t *testing.T) {
	b, err := (&Body{PtzControl: &PtzControl{Version: XMLVersion, ChannelID: 0, Speed: 32, Command: "left"}}).Marshal()
	require.NoError(t, err)
	require.Contains(t, string(b), "<channelId>0</channelId>")
	require.Contains(t, string(b), "<command>left</command>")
}

func TestPTZPresetsRoundTrip(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	require.NoError(t, c.PTZSetPreset(3, "door"))
	list, err := c.PTZPresets()
	require.NoError(t, err)
	require.Equal(t, []Preset{{ID: 3, Name: "door"}}, list)

	require.NoError(t, c.PTZGotoPreset(3))
	require.Contains(t, f.events(), "preset:toPos:3")

	err = c.PTZGotoPreset(9)
	require.Error(t, err)
	require.Contains(t, err.Error(), "19")
	require.Contains(t, err.Error(), "400")
}

func TestPTZCaps(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)
	caps, err := c.PTZCaps()
	require.NoError(t, err)
	require.Equal(t, PTZCaps{PanTilt: true, Zoom: true, ZoomMin: 0, ZoomMax: 33}, caps)

	f2 := newFakeCamera(t)
	f2.noPTZ = true
	c2 := dialFake(t, f2, fastConfig())
	_, err = c2.Login()
	require.NoError(t, err)
	caps, err = c2.PTZCaps()
	require.NoError(t, err)
	require.Equal(t, PTZCaps{}, caps)
}

func TestZoomTo(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	require.NoError(t, c.ZoomTo(20))
	require.Equal(t, []string{"zoom:20"}, f.events())
	zf, err := c.ZoomFocus()
	require.NoError(t, err)
	require.Equal(t, uint32(20), zf.Zoom.Cur)
}
