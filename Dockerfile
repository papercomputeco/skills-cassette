FROM golang:1.25 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 go build -trimpath -ldflags="$LDFLAGS" -o /out/skills-cassette ./cli/skills-cassette

FROM gcr.io/distroless/static-debian12:nonroot

EXPOSE 9998
COPY --from=build /out/skills-cassette /usr/local/bin/skills-cassette
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/skills-cassette"]
CMD ["serve", "--listen", "0.0.0.0:9998"]
