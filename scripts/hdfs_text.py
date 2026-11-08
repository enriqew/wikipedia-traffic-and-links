"""Write the files matching an HDFS glob to stdout, decompressing zlib job output (.deflate).

`hdfs dfs -text` would do it, but the image's log4j.properties sends the client's INFO log to
stdout (`log4j.rootLogger: INFO, stdout`), so its codec messages land among the data. This reads
each file with `hdfs dfs -cat`, which logs nothing, in sorted path order, as a stream.

Runs inside the client container:
    docker compose run --rm -T client python3 /scripts/hdfs_text.py '/out/graph/x/graph/adjacency-r-*'
"""

import subprocess
import sys
import zlib

BLOCK = 8 << 20


def main() -> None:
    listing = subprocess.run(["hdfs", "dfs", "-ls", "-C", sys.argv[1]], capture_output=True, text=True, check=True)
    out = sys.stdout.buffer
    for path in sorted(listing.stdout.split()):
        if not path.startswith("/"):
            raise SystemExit(f"not a path in the listing: {path!r}")
        cat = subprocess.Popen(["hdfs", "dfs", "-cat", path], stdout=subprocess.PIPE)
        inflate = zlib.decompressobj() if path.endswith(".deflate") else None
        for block in iter(lambda: cat.stdout.read(BLOCK), b""):
            out.write(inflate.decompress(block) if inflate else block)
        if inflate:
            out.write(inflate.flush())
            if not inflate.eof:
                raise SystemExit(f"{path}: the zlib stream ends early")
        if cat.wait() != 0:
            raise SystemExit(f"hdfs dfs -cat {path} failed")
    out.flush()


if __name__ == "__main__":
    main()
