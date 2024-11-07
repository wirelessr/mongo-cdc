# 構建階段
FROM golang:1.21-alpine AS builder

# 安裝必要的系統依賴
RUN apk add --no-cache gcc g++ musl-dev pkgconfig git librdkafka-dev

WORKDIR /app

# 複製 go mod 文件
COPY go.mod go.sum ./

RUN go mod download

# 複製源代碼
COPY . .

# 構建應用
RUN CGO_ENABLED=1 go build -tags musl -o main .

# 運行階段
FROM alpine:latest

WORKDIR /app

# 從構建階段複製編譯好的程序
COPY --from=builder /app/main .

# 運行應用
CMD ["./main"]
