FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o /out/mesh-demo .

FROM alpine:3.21
COPY --from=build /out/mesh-demo /usr/local/bin/mesh-demo
EXPOSE 8080
ENTRYPOINT ["mesh-demo"]
