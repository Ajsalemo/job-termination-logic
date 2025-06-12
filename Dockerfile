FROM golang:1.23.4-alpine3.21

WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/local/bin/app

# Run
CMD ["/usr/local/bin/app"]