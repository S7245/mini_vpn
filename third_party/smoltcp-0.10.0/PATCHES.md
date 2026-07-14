# Local patches

This directory vendors the exact `smoltcp 0.10.0` source selected by
`Cargo.lock`.

mini_vpn's Knife14h10d16 local-uplink-window stage adds an optional TCP
receive-window limit that separates physical receive-buffer storage from the
advertised and accepted TCP window. Default sockets preserve upstream
behavior. See:

`docs/tech/2026-07-13-knife14h10d16-local-uplink-window-service-architecture-spec.md`
