FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /loader .

FROM gcr.io/distroless/static-debian12
COPY --from=build /loader /loader
ENTRYPOINT ["/loader"]
