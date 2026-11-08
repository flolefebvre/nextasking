# Every key lives under one hash tag

Every key is `{<prefix>}:…` and every script receives its keys through `KEYS`, never builds one in Lua, so the layout already fits Redis Cluster at one slot. 0.2.0 ships no Cluster client because nothing needed it yet; adding one later is additive, while dropping the braces or deriving a key inside a script would break that silently, since CI runs a single node. Neither is dead weight. Decided in [Failover, Cluster and supported servers](https://github.com/flolefebvre/nextasking/issues/133).
