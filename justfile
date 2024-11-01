maelstrom *FLAGS: bootstrap bin
    ./maelstrom/maelstrom {{ FLAGS }}

echo: bootstrap bin
    ./maelstrom/maelstrom test -w echo --bin target/release/fly-systems-challenge --time-limit 10 --node-count 1

unique-ids: bootstrap bin
    ./maelstrom/maelstrom test -w unique-ids --bin target/release/fly-systems-challenge --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast: bootstrap bin
    ./maelstrom/maelstrom test -w broadcast --bin target/release/fly-systems-challenge --node-count 5 --time-limit 20 --rate 10

bootstrap:
    #!/usr/bin/env bash
    TOPLEVEL=$(git rev-parse --show-toplevel)

    # ensure $TOPLEVEL is set
    if [ -z $TOPLEVEL ]; then
      echo "Error: not in a git repository"
      exit 1
    fi

    if [ ! -d $TOPLEVEL/maelstrom ]; then
      # do nothing, maelstrom is already here
      if [ ! -e $TOPLEVEL/maelstrom.tar.bz2 ]; then
        wget wget https://github.com/jepsen-io/maelstrom/releases/latest/download/maelstrom.tar.bz2
      fi
      tar -xvf maelstrom.tar.bz2
    fi

bin:
    cargo build --release

clean:
    @rm -rf maelstrom
    @rm -f  maelstrom.tar.bz2
    @echo "cleaned up"
