# Flow2API

<div align="center">

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-0.119.0-green.svg)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/docker-supported-blue.svg)](https://www.docker.com/)

**Dịch vụ API tương thích OpenAI đầy đủ tính năng, cung cấp giao diện thống nhất cho Flow**

</div>

## ✨ Tính năng cốt lõi

- 🎨 **Text-to-Image (Tạo ảnh từ văn bản)** / **Image-to-Image (Tạo ảnh từ ảnh)**
- 🎬 **Text-to-Video (Tạo video từ văn bản)** / **Image-to-Video (Tạo video từ ảnh)**
- 🎞️ **Video khung đầu/cuối (First/Last Frame)**
- 🔄 **Tự động refresh AT/ST** - AT hết hạn tự động làm mới, ST hết hạn tự động cập nhật qua trình duyệt (chế độ personal)
- 📊 **Hiển thị số dư** - Truy vấn và hiển thị VideoFX Credits theo thời gian thực
- 🚀 **Cân bằng tải (Load Balancing)** - Xoay vòng nhiều Token và kiểm soát đồng thời
- 🌐 **Hỗ trợ Proxy** - Hỗ trợ proxy HTTP/SOCKS5
- 📱 **Giao diện quản trị Web** - Quản lý Token và cấu hình trực quan
- 🎨 **Hội thoại liên tục khi tạo ảnh**
- 🧩 **Tương thích request chính thức của Gemini** - Hỗ trợ `generateContent` / `streamGenerateContent`, `systemInstruction`, `contents.parts.text/inlineData/fileData`
- ✅ **Format chính thức Gemini đã kiểm chứng tạo ảnh thực tế** - Đã dùng Token thật xác minh `/models/{model}:generateContent` trả về đúng `candidates[].content.parts[].inlineData` theo format chính thức

## 🚀 Bắt đầu nhanh

### Yêu cầu trước khi cài

- Docker và Docker Compose (khuyến nghị)
- Hoặc Python 3.8+

- Do Flow có thêm lớp xác thực captcha, bạn có thể chọn dùng captcha bằng trình duyệt hoặc dịch vụ giải captcha bên thứ ba:
Đăng ký [YesCaptcha](https://yescaptcha.com/i/13Xd8K) để lấy api key, sau đó điền vào mục ```YesCaptcha API Key``` trong trang cấu hình hệ thống.
- File `docker-compose.yml` mặc định khuyến nghị dùng kèm dịch vụ bên thứ ba (yescaptcha/capmonster/ezcaptcha/capsolver).
Nếu cần captcha bằng trình duyệt có giao diện (browser/personal) trong Docker, hãy dùng file `docker-compose.headed.yml` bên dưới.

- Extension trình duyệt tự động cập nhật ST: [Flow2API-Token-Updater](https://github.com/TheSmallHanCat/Flow2API-Token-Updater)

### Cách 1: Triển khai bằng Docker (khuyến nghị)

#### Chế độ chuẩn (không dùng proxy)

```bash
# Clone dự án
git clone https://github.com/TheSmallHanCat/flow2api.git
cd flow2api

# Khởi động dịch vụ
docker-compose up -d

# Xem log
docker-compose logs -f
```

> Lưu ý: Compose đã mặc định mount `./tmp:/app/tmp`. Nếu đặt timeout cache là `0`, nghĩa là "không tự động hết hạn xóa"; nếu muốn file cache được giữ lại sau khi build lại container, cũng cần giữ mount `tmp` này.

#### Chế độ WARP (dùng proxy)

```bash
# Khởi động với proxy WARP
docker-compose -f docker-compose.warp.yml up -d

# Xem log
docker-compose -f docker-compose.warp.yml logs -f
```

#### Chế độ Docker headed captcha (browser / personal)

> Phù hợp khi bạn cần desktop ảo hóa và muốn dùng captcha trình duyệt có giao diện trong container.
> Chế độ này mặc định khởi động `Xvfb + Fluxbox` để hiển thị trong container, và đặt `ALLOW_DOCKER_HEADED_CAPTCHA=true`.
> Chỉ mở port ứng dụng, không cung cấp bất kỳ port remote desktop nào.
> Trình duyệt tích hợp `personal` hiện mặc định khởi động ở chế độ có giao diện (headed); nếu muốn tạm thời chuyển về headless, có thể đặt biến môi trường `PERSONAL_BROWSER_HEADLESS=true`.

```bash
# Khởi động chế độ headed (lần đầu khuyến nghị kèm --build)
docker compose -f docker-compose.headed.yml up -d --build

# Xem log
docker compose -f docker-compose.headed.yml logs -f
```

- Port API: `8000`
- Sau khi vào trang quản trị, đặt phương thức captcha thành `browser` hoặc `personal`

### Cách 2: Triển khai cục bộ

```bash
# Clone dự án
git clone https://github.com/TheSmallHanCat/flow2api.git
cd flow2api

# Tạo môi trường ảo
python -m venv venv

# Kích hoạt môi trường ảo
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt

# Khởi động dịch vụ
python main.py
```

### Truy cập lần đầu

Sau khi dịch vụ khởi động, truy cập trang quản trị: **http://localhost:8000**, hãy đổi mật khẩu ngay sau khi đăng nhập lần đầu!

- **Tên đăng nhập**: `admin`
- **Mật khẩu**: `admin`

### Trang test model

Truy cập **http://localhost:8000/test** để mở trang test model tích hợp sẵn, hỗ trợ:

- Duyệt tất cả model khả dụng theo phân loại (tạo ảnh, text/image sang video, video nhiều ảnh, upscale video, v.v.)
- Nhập prompt để test nhanh, hiển thị tiến trình tạo theo dạng streaming
- Kịch bản image-to-image / image-to-video hỗ trợ upload ảnh
- Xem trực tiếp ảnh hoặc video sau khi tạo xong

## 📋 Các model được hỗ trợ

### Tạo ảnh

| Tên model | Mô tả | Kích thước |
|---------|--------|--------|
| `gemini-2.5-flash-image-landscape` | Image/Text-to-Image | Ngang |
| `gemini-2.5-flash-image-portrait` | Image/Text-to-Image | Dọc |
| `gemini-3.0-pro-image-landscape` | Image/Text-to-Image | Ngang |
| `gemini-3.0-pro-image-portrait` | Image/Text-to-Image | Dọc |
| `gemini-3.0-pro-image-square` | Image/Text-to-Image | Vuông |
| `gemini-3.0-pro-image-four-three` | Image/Text-to-Image | Ngang 4:3 |
| `gemini-3.0-pro-image-three-four` | Image/Text-to-Image | Dọc 3:4 |
| `gemini-3.0-pro-image-landscape-2k` | Image/Text-to-Image (2K) | Ngang |
| `gemini-3.0-pro-image-portrait-2k` | Image/Text-to-Image (2K) | Dọc |
| `gemini-3.0-pro-image-square-2k` | Image/Text-to-Image (2K) | Vuông |
| `gemini-3.0-pro-image-four-three-2k` | Image/Text-to-Image (2K) | Ngang 4:3 |
| `gemini-3.0-pro-image-three-four-2k` | Image/Text-to-Image (2K) | Dọc 3:4 |
| `gemini-3.0-pro-image-landscape-4k` | Image/Text-to-Image (4K) | Ngang |
| `gemini-3.0-pro-image-portrait-4k` | Image/Text-to-Image (4K) | Dọc |
| `gemini-3.0-pro-image-square-4k` | Image/Text-to-Image (4K) | Vuông |
| `gemini-3.0-pro-image-four-three-4k` | Image/Text-to-Image (4K) | Ngang 4:3 |
| `gemini-3.0-pro-image-three-four-4k` | Image/Text-to-Image (4K) | Dọc 3:4 |
| `imagen-4.0-generate-preview-landscape` | Image/Text-to-Image | Ngang |
| `imagen-4.0-generate-preview-portrait` | Image/Text-to-Image | Dọc |
| `gemini-3.1-flash-image-landscape` | Image/Text-to-Image | Ngang |
| `gemini-3.1-flash-image-portrait` | Image/Text-to-Image | Dọc |
| `gemini-3.1-flash-image-square` | Image/Text-to-Image | Vuông |
| `gemini-3.1-flash-image-four-three` | Image/Text-to-Image | Ngang 4:3 |
| `gemini-3.1-flash-image-three-four` | Image/Text-to-Image | Dọc 3:4 |
| `gemini-3.1-flash-image-landscape-2k` | Image/Text-to-Image (2K) | Ngang |
| `gemini-3.1-flash-image-portrait-2k` | Image/Text-to-Image (2K) | Dọc |
| `gemini-3.1-flash-image-square-2k` | Image/Text-to-Image (2K) | Vuông |
| `gemini-3.1-flash-image-four-three-2k` | Image/Text-to-Image (2K) | Ngang 4:3 |
| `gemini-3.1-flash-image-three-four-2k` | Image/Text-to-Image (2K) | Dọc 3:4 |
| `gemini-3.1-flash-image-landscape-4k` | Image/Text-to-Image (4K) | Ngang |
| `gemini-3.1-flash-image-portrait-4k` | Image/Text-to-Image (4K) | Dọc |
| `gemini-3.1-flash-image-square-4k` | Image/Text-to-Image (4K) | Vuông |
| `gemini-3.1-flash-image-four-three-4k` | Image/Text-to-Image (4K) | Ngang 4:3 |
| `gemini-3.1-flash-image-three-four-4k` | Image/Text-to-Image (4K) | Dọc 3:4 |

### Tạo video

#### Text-to-Video (T2V)
⚠️ **Không hỗ trợ upload ảnh**

| Tên model | Mô tả | Kích thước |
|---------|---------|--------|
| `veo_3_1_t2v_fast_portrait` | Text-to-Video | Dọc |
| `veo_3_1_t2v_fast_landscape` | Text-to-Video | Ngang |
| `veo_3_1_t2v_fast_portrait_ultra` | Text-to-Video | Dọc |
| `veo_3_1_t2v_fast_ultra` | Text-to-Video | Ngang |
| `veo_3_1_t2v_fast_portrait_ultra_relaxed` | Text-to-Video | Dọc |
| `veo_3_1_t2v_fast_ultra_relaxed` | Text-to-Video | Ngang |
| `veo_3_1_t2v_portrait` | Text-to-Video | Dọc |
| `veo_3_1_t2v_landscape` | Text-to-Video | Ngang |
| `veo_3_1_t2v_lite_portrait` | Text-to-Video Lite | Dọc |
| `veo_3_1_t2v_lite_landscape` | Text-to-Video Lite | Ngang |

#### Model khung đầu/cuối (I2V - Image to Video)
📸 **Hỗ trợ 1-2 ảnh: 1 ảnh làm khung đầu, 2 ảnh làm khung đầu + khung cuối**

> 💡 **Tự động thích ứng**: Hệ thống sẽ tự chọn model_key tương ứng theo số lượng ảnh
> - **Chế độ 1 khung** (1 ảnh): Dùng khung đầu để tạo video
> - **Chế độ 2 khung** (2 ảnh): Dùng khung đầu + khung cuối để tạo video chuyển cảnh
> - `veo_3_1_i2v_lite_*` chỉ hỗ trợ **1 ảnh** khung đầu
> - `veo_3_1_interpolation_lite_*` chỉ hỗ trợ **2 ảnh** khung đầu + khung cuối

| Tên model | Mô tả | Kích thước |
|---------|---------|--------|
| `veo_3_1_i2v_s_fast_portrait_fl` | Image-to-Video | Dọc |
| `veo_3_1_i2v_s_fast_fl` | Image-to-Video | Ngang |
| `veo_3_1_i2v_s_fast_portrait_ultra_fl` | Image-to-Video | Dọc |
| `veo_3_1_i2v_s_fast_ultra_fl` | Image-to-Video | Ngang |
| `veo_3_1_i2v_s_fast_portrait_ultra_relaxed` | Image-to-Video | Dọc |
| `veo_3_1_i2v_s_fast_ultra_relaxed` | Image-to-Video | Ngang |
| `veo_3_1_i2v_s_portrait` | Image-to-Video | Dọc |
| `veo_3_1_i2v_s_landscape` | Image-to-Video | Ngang |
| `veo_3_1_i2v_lite_portrait` | Image-to-Video Lite (chỉ khung đầu) | Dọc |
| `veo_3_1_i2v_lite_landscape` | Image-to-Video Lite (chỉ khung đầu) | Ngang |
| `veo_3_1_interpolation_lite_portrait` | Image-to-Video Lite (chuyển cảnh đầu/cuối) | Dọc |
| `veo_3_1_interpolation_lite_landscape` | Image-to-Video Lite (chuyển cảnh đầu/cuối) | Ngang |

#### Tạo video từ nhiều ảnh (R2V - Reference Images to Video)
🖼️ **Hỗ trợ nhiều ảnh**

> **Cập nhật 2026-03-06**
>
> - Đã đồng bộ request body `R2V` phiên bản mới từ upstream
> - `textInput` đã chuyển sang `structuredPrompt.parts`
> - Thêm `mediaGenerationContext.batchId` ở cấp cao nhất
> - Thêm `useV2ModelConfig: true` ở cấp cao nhất
> - Model `R2V` ngang / dọc dùng chung một request body phiên bản mới
> - `videoModelKey` upstream của `R2V` ngang đã chuyển sang dạng `*_landscape`
> - Theo giao thức upstream hiện tại, `referenceImages` hiện tối đa **3 ảnh**

| Tên model | Mô tả | Kích thước |
|---------|---------|--------|
| `veo_3_1_r2v_fast_portrait` | Image-to-Video | Dọc |
| `veo_3_1_r2v_fast` | Image-to-Video | Ngang |
| `veo_3_1_r2v_fast_portrait_ultra` | Image-to-Video | Dọc |
| `veo_3_1_r2v_fast_ultra` | Image-to-Video | Ngang |
| `veo_3_1_r2v_fast_portrait_ultra_relaxed` | Image-to-Video | Dọc |
| `veo_3_1_r2v_fast_ultra_relaxed` | Image-to-Video | Ngang |

#### Model nâng cấp video (Upsample)

| Tên model | Mô tả | Đầu ra |
|---------|---------|--------|
| `veo_3_1_t2v_fast_portrait_4k` | Upscale Text-to-Video | 4K |
| `veo_3_1_t2v_fast_4k` | Upscale Text-to-Video | 4K |
| `veo_3_1_t2v_fast_portrait_ultra_4k` | Upscale Text-to-Video | 4K |
| `veo_3_1_t2v_fast_ultra_4k` | Upscale Text-to-Video | 4K |
| `veo_3_1_t2v_fast_portrait_1080p` | Upscale Text-to-Video | 1080P |
| `veo_3_1_t2v_fast_1080p` | Upscale Text-to-Video | 1080P |
| `veo_3_1_t2v_fast_portrait_ultra_1080p` | Upscale Text-to-Video | 1080P |
| `veo_3_1_t2v_fast_ultra_1080p` | Upscale Text-to-Video | 1080P |
| `veo_3_1_i2v_s_fast_portrait_ultra_fl_4k` | Upscale Image-to-Video | 4K |
| `veo_3_1_i2v_s_fast_ultra_fl_4k` | Upscale Image-to-Video | 4K |
| `veo_3_1_i2v_s_fast_portrait_ultra_fl_1080p` | Upscale Image-to-Video | 1080P |
| `veo_3_1_i2v_s_fast_ultra_fl_1080p` | Upscale Image-to-Video | 1080P |
| `veo_3_1_r2v_fast_portrait_ultra_4k` | Upscale video nhiều ảnh | 4K |
| `veo_3_1_r2v_fast_ultra_4k` | Upscale video nhiều ảnh | 4K |
| `veo_3_1_r2v_fast_portrait_ultra_1080p` | Upscale video nhiều ảnh | 1080P |
| `veo_3_1_r2v_fast_ultra_1080p` | Upscale video nhiều ảnh | 1080P |

## 📡 Ví dụ sử dụng API (phải dùng streaming)

> Ngoài ví dụ `OpenAI-compatible` bên dưới, dịch vụ còn hỗ trợ format chính thức của Gemini:
> - `POST /v1beta/models/{model}:generateContent`
> - `POST /models/{model}:generateContent`
> - `POST /v1beta/models/{model}:streamGenerateContent`
> - `POST /models/{model}:streamGenerateContent`
>
> Format chính thức Gemini hỗ trợ các phương thức xác thực sau:
> - `Authorization: Bearer <api_key>`
> - `x-goog-api-key: <api_key>`
> - `?key=<api_key>`
>
> Request body ảnh theo format chính thức Gemini đã tương thích:
> - `systemInstruction`
> - `contents[].parts[].text`
> - `contents[].parts[].inlineData`
> - `contents[].parts[].fileData.fileUri`
> - `generationConfig.responseModalities`
> - `generationConfig.imageConfig.aspectRatio`
> - `generationConfig.imageConfig.imageSize`

### generateContent chính thức Gemini (text-to-image)

> Đã kiểm chứng bằng Token thật.
> Nếu cần trả về streaming, có thể thay đường dẫn thành `:streamGenerateContent?alt=sse`.

```bash
curl -X POST "http://localhost:8000/models/gemini-3.1-flash-image:generateContent" \
  -H "x-goog-api-key: han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "systemInstruction": {
      "parts": [
        {
          "text": "Return an image only."
        }
      ]
    },
    "contents": [
      {
        "role": "user",
        "parts": [
          {
            "text": "Một quả táo đỏ đặt trên bàn gỗ, ánh sáng studio, nền tối giản"
          }
        ]
      }
    ],
    "generationConfig": {
      "responseModalities": ["IMAGE"],
      "imageConfig": {
        "aspectRatio": "1:1",
        "imageSize": "1K"
      }
    }
  }'
```

### Text-to-Image

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image-landscape",
    "messages": [
      {
        "role": "user",
        "content": "Một chú mèo đáng yêu đang chơi đùa trong vườn hoa"
      }
    ],
    "stream": true
  }'
```

### Image-to-Image

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.1-flash-image-landscape",
    "messages": [
      {
        "role": "user",
        "content": [
          {
            "type": "text",
            "text": "Biến ảnh này thành phong cách tranh màu nước"
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64,<base64_encoded_image>"
            }
          }
        ]
      }
    ],
    "stream": true
  }'
```

### Text-to-Video

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "veo_3_1_t2v_fast_landscape",
    "messages": [
      {
        "role": "user",
        "content": "Một chú mèo con đang đuổi bướm trên bãi cỏ"
      }
    ],
    "stream": true
  }'
```

### Tạo video từ khung đầu/cuối

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "veo_3_1_i2v_s_fast_fl_landscape",
    "messages": [
      {
        "role": "user",
        "content": [
          {
            "type": "text",
            "text": "Chuyển cảnh từ ảnh thứ nhất sang ảnh thứ hai"
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64,<base64_khung_đầu>"
            }
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64,<base64_khung_cuối>"
            }
          }
        ]
      }
    ],
    "stream": true
  }'
```

### Tạo video từ nhiều ảnh

> `R2V` sẽ được server tự động lắp request body video phiên bản mới, bên gọi vẫn dùng input tương thích OpenAI như bình thường.
> Server sẽ tự map `R2V` ngang sang upstream model key `*_landscape` mới nhất.
> Hiện tối đa **3 ảnh tham chiếu**.

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "veo_3_1_r2v_fast_portrait",
    "messages": [
      {
        "role": "user",
        "content": [
          {
            "type": "text",
            "text": "Dựa trên nhân vật và bối cảnh từ ba ảnh tham chiếu, tạo một video dọc với chuyển động camera mượt mà"
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64/<base64_ảnh_tham_chiếu_1>"
            }
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64/<base64_ảnh_tham_chiếu_2>"
            }
          },
          {
            "type": "image_url",
            "image_url": {
              "url": "data:image/jpeg;base64/<base64_ảnh_tham_chiếu_3>"
            }
          }
        ]
      }
    ],
    "stream": true
  }'
```

---

## 📄 Giấy phép

Dự án này sử dụng giấy phép MIT. Xem chi tiết tại file [LICENSE](LICENSE).

---

## 🙏 Lời cảm ơn

- [PearNoDec](https://github.com/PearNoDec) đã cung cấp giải pháp captcha YesCaptcha
- [raomaiping](https://github.com/raomaiping) đã cung cấp giải pháp captcha headless
Cảm ơn sự ủng hộ của tất cả contributor và người dùng!

---

## 📞 Liên hệ

- Gửi Issue: [GitHub Issues](https://github.com/TheSmallHanCat/flow2api/issues)

---

