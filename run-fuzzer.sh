#!/bin/bash

# SAMPLE RUN:
#       ./run-rigfuzz.py FlightDistance faulty 86400 --email=ahmad35@vt.edu --compile=True

#export JAVA_HOME=~/.jdks/corretto-1.8.0_332
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# sudo update-alternatives --set java $JAVA_HOME/bin/java || exit 1
# sudo update-alternatives --set javac $JAVA_HOME/bin/javac || exit 1
exitScript() {
    mv ~/jazzerresults src/main/scala
    exit 1;
}

#mv src/main/scala/jazzerresults ~ # sbt gets stuck in infinite loop so move this out of directory
#sbt assembly || exit 1 # exitScript
#mv ~/jazzerresults src/main/scala

NAME=$1
MUTANT_NAME=$2
DURATION=$3
shift 3
DATASETS=$@

PATH_SCALA_SRC="src/main/scala/examples/fwa/$NAME.scala"
PATH_INSTRUMENTED_CLASSES="examples/fwa/$NAME*"
DIR_NATURALFUZZ_OUT="target/naturalfuzz-output/$MUTANT_NAME"
JAR_NAME=ProvFuzz-assembly-1.0.jar

rm -rf $DIR_NATURALFUZZ_OUT
mkdir -p graphs $DIR_NATURALFUZZ_OUT/{scoverage-results,report,log,reproducers,crashes} || exit 1


sbt assembly || exit 1

java -cp  target/scala-2.12/$JAR_NAME \
          refactor.RunTransformer \
          $NAME \
          || exit 1

sbt assembly || exit 1
# sbt assembly || exit 1

#java -cp  target/scala-2.12/$JAR_NAME \
#          runners.RunFuzzerJar
java -cp  target/scala-2.12/ProvFuzz-assembly-1.0.jar \
          utils.ScoverageInstrumenter \
          $PATH_SCALA_SRC \
          $DIR_NATURALFUZZ_OUT/scoverage-results/referenceProgram || exit 1

pushd target/scala-2.12/classes || exit 1
jar uvf ../ProvFuzz-assembly-1.0.jar \
        $PATH_INSTRUMENTED_CLASSES \
        || exit 1
popd || exit 1

date > $DIR_NATURALFUZZ_OUT/start.time

java -cp  target/scala-2.12/ProvFuzz-assembly-1.0.jar \
          runners.RunFuzzerJar \
          $NAME \
          local[*] \
          $DURATION \
          $DIR_NATURALFUZZ_OUT \
          $DATASETS

date > $DIR_NATURALFUZZ_OUT/end.time
cat $DIR_NATURALFUZZ_OUT/scoverage-results/referenceProgram/coverage.tuples

python3 gen_graph.py \
        --coords-file $DIR_NATURALFUZZ_OUT/scoverage-results/referenceProgram/coverage.tuples \
        --outfile graphs/graph-$NAME-coverage.png \
        --title " Coverage: $NAME" \
        --x-label "Time (s)" \
        --y-label "Statement Coverage (%)" && echo "Graphs generated!"

rm -rf src/main/scala/examples/{fwa,instrumented}
rm -rf pickled
