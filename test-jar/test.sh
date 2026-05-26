#!/bin/bash

set -eou pipefail

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )

echo "Java version $jvm_version"

cd "$(dirname "$0")"

version=$(sed -n 's/^(def version "\(.*\)")/\1/p' ../project.clj | head -n1)
jar="../target/datalevin-${version}.jar"

if [[ -z "$version" || ! -f "$jar" ]]; then
    echo "Datalevin jar not found: $jar" >&2
    exit 1
fi

clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED \
        -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        -Sdeps "{:deps {datalevin/datalevin {:local/root \"$jar\"}}}" \
        -M -m test-jar.core
