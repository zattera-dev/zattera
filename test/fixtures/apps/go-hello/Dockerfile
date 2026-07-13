FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o /out/go-hello .

FROM alpine:3.21
COPY --from=build /out/go-hello /usr/local/bin/go-hello
EXPOSE 8080
ENTRYPOINT ["go-hello"]
