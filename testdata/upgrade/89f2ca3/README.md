# 89f2ca3 upgrade fixture

Tag: sha-89f2ca3 (`.github/workflows/publish.yml` names a build by its commit; this repository has
no version tag).
Commit: 89f2ca35f72169c700f90ebd6ab139c98627272e
Command: at that commit, place `gen/gen_test.go.txt` as
`internal/store/kubestore/gen_fixture_test.go` and run
`FIXTURE_OUT=$PWD/testdata/upgrade/89f2ca3/configmaps.json go test ./internal/store/kubestore -run TestGenFixture`,
then commit the file it writes. `gen/` is outside the Go build, so `go test ./...` never runs it.
Recorded objects: `configmaps.json` is one `v1` List of the five ConfigMaps the old kubestore wrote
in namespace `vws`: one imported 8192-bit `ssh-rsa` key pair (`legacy-rsa-8192`), one instance whose
`key_public` carries the same key, their two request-id indexes, and the key-name claim. The commit
above is the parent of the #112 fix, so its import path accepted an RSA key above the current 4096-bit
cap and the current read path must keep decoding the record (vws-compute#112).

Regenerating a record is allowed only at a release bump (vws-deploy#396). A change that cannot read
this record fails `TestPreviousReleaseRecordsDecode` in the normal CI `test` job.
