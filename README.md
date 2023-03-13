# fluent-bit-mongo

This module makes the link between fluent-bit & mongoDB, in order to save the logs with metadata converted into a defined format.

If you want to save logs with other data and/or for a new type of Mongo document, it is necessary to modify the file `pkg/entry/mongo/document.go`.

This module has 2 Github Actions:
- check: allows you to check the auto tests on the branch. This action is launched systematically during the push on master or manually via https://github.com/saagie/fluent-bit-mongo/actions/workflows/check.yml
- release: allows to create the compilation and the build of an image corresponding to the branch. This action is launched on the push of a git tag. It is also possible to launch it manually via https://github.com/saagie/fluent-bit-mongo/blob/master/.github/workflows/release.yml The image thus created will be uploaded to the DockerHub defined in the settings. Note that a git tag is mandatory for this action to publish the image on DockerHub.