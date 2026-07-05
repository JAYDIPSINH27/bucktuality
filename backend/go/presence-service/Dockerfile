FROM golang:1.26.4-alpine AS build

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o presence-service .

FROM alpine:latest

WORKDIR /app

COPY --from=build /app/presence-service .

EXPOSE 8084

ENTRYPOINT ["./presence-service"]