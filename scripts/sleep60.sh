#!/bin/sh
# A stand-in for a long external program.
# `forge submit --operation exec --exec <this script>` replaces a worker
# with this process image via execve. The controller then has a real child
# to time out, cancel, and reap. The script ignores its arguments.
sleep 60
