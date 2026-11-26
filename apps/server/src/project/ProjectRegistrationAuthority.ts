// A JSON mutation cannot carry this in-process capability. Only the authenticated
// supervisor RPC constructs it; it grants metadata registration, never file access.
export const registrationAuthority = Symbol("iCore project registration");
