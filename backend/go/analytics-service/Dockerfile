FROM golang:1.26.4-alpine AS build

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o analytics-service .

FROM alpine:latest

WORKDIR /app

COPY --from=build /app/analytics-service .

EXPOSE 8083

ENTRYPOINT ["./analytics-service"]