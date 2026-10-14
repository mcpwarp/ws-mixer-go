// Package mcpwarp is the mcpwarp application layer on top of the ws-mixer
// client (ws-mixer-spec docs/MCPWARP.md): the `app.body.mcpwarp` envelope
// (§2), identity and service registration with take-over (§3), HTTP request
// bridging over ws-mixer streams (§4), and the SDK profile on top: Expose
// (§5), the tunnel controller NewTunnel (§6) and DescribeStatus (§7).
//
// The wsmixer core package stays free of mcpwarp types; everything here is
// built on its public Client, Conn and Stream API.
package mcpwarp
