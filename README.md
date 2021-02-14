# Running

```bash
git clone git@github.com:tkroman/ddmf.git && \
  cd ddmf && \ 
  sbt docker && \
  docker run -p 8080:8080 -e NUMBER_OF_NODES=4 -e HISTORY_SIZE=1024 tkroman/ddmf
```
