# Tasks and to-dos
## Signal server
- Scrap the HTTP server idea, `matchbox` it is.
## Others
- One of the nodes is the NTP server. Probably will bolt that onto the
signaling server node as well.
- `nftables` configurations to rate-limit... Wait, no, `ufw` has limit rules.
Use that one, the syntax is a lot less cursed than `nft` by itself... But, you
will need to install `nft` either way.
