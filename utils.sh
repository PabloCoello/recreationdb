DM_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WO_DIR=/opt/recreationdb
DK_IMG=recreationdb-env

function execute () {
	docker run -e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 --rm -it -v  "${DM_DIR}":"${WO_DIR}" --network host "${DK_IMG}" python3 "${WO_DIR}" flikr.py $conf run
}

for conf in "$@"
do
    execute
done