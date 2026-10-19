package mcpwarp

// ServiceState is one service's state in a ServiceGroupStatus (§6A.4).
type ServiceState string

const (
	ServicePending      ServiceState = "pending"
	ServiceActive       ServiceState = "active"
	ServiceDisabled     ServiceState = "disabled"
	ServiceRejected     ServiceState = "rejected"
	ServiceTakenOver    ServiceState = "taken_over"
	ServiceUnregistered ServiceState = "unregistered"
)

// ServiceStatus is one service of a group (§6A.4). ID is the last server id
// learned for the name; URL is set only while Active; Code and LastError are
// set together. Code is DISABLED, TAKEN_OVER, or for Rejected the wire code
// as received (a registered.errors[].code or a whole-frame error.code), not
// necessarily a StatusCode.
//
// A Disabled service tells its cause by Code: "DISABLED" (string(CodeDisabled))
// when the dashboard disabled it, by a disable op or a SERVER_DISABLED
// refusal alike; "" when ServiceGroup.Disable disabled it locally.
type ServiceStatus struct {
	State     ServiceState
	ID        string
	URL       string
	Code      string
	LastError string
}

// ServiceGroupStatus is a group's status (§6A.4): State, Code and LastError
// describe the connection as TunnelStatus does (never a URL); Services holds
// every configured name and every removed one still waiting for its
// unregistered, and is empty (never nil) when off.
type ServiceGroupStatus struct {
	State     TunnelState
	Code      StatusCode
	LastError string
	Services  map[string]ServiceStatus
}
