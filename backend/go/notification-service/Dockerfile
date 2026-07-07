FROM golang:1.26.4-alpine AS build

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o notification-service .

FROM alpine:latest

WORKDIR /app

COPY --from=build /app/notification-service .

EXPOSE 8085

ENTRYPOINT ["./notification-service"]