# OBVFUTPORT-v2 Version Control Discipline

Track only the operating layer:

- strategy/runtime scripts
- config templates
- adaptive override JSON versions
- deployment/systemd templates
- SOP and audit documents
- small manifests and checksums

Do not track bulky or mutable runtime data:

- compact target streams
- instrument ledgers
- Matrix event/state files
- dashboard state files
- archive extracts
- replay workspaces
- telemetry/log files

Every promoted adaptive override must have:

- a versioned file name
- source run path
- symbol count
- quarantined symbol list
- checksum
- promotion notes
- rollback pointer to the previous active version

Daily EOD runs append state and produce manifests. Weekly promotion runs may create a new override version. A full Aug10+ replay/reseed remains approval-gated unless it is only installing already-produced selected-candidate artifacts.

