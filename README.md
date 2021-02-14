# Running

If you don't have Scala & sbt installed, please refer to the [one-click install for scala
](https://www.scala-lang.org/2020/06/29/one-click-install.html) docs. TLDR:
```bash
curl -Lo cs https://git.io/coursier-cli-linux && chmod +x cs && ./cs setup
```

Then, clone this repository, build & run Docker image:

```bash
git clone git@github.com:tkroman/ddmf.git && \
  cd ddmf && \ 
  sbt docker && \
  docker run -p 8080:8080 -e NUMBER_OF_NODES=4 -e HISTORY_SIZE=1024 tkroman/ddmf
```
