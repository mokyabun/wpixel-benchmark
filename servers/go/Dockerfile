FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /server .

FROM gcr.io/distroless/static-debian12
LABEL org.wsbench.runtime="go" org.wsbench.implementation="Go 1.24 + coder/websocket 1.8"
COPY --from=build /server /server
ENV PORT=8080 CORES=1
EXPOSE 8080
ENTRYPOINT ["/server"]
