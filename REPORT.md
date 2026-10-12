# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Công Thịnh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602781` |
| GitHub | `2003congthinh` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06570` |

## Kết quả cuối

- `dataset_version` của bạn: `7a789a787ab8`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **643 / 215 / 161** (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).
  Ảnh bài tập sập của mình: `crash_1_graph_red.png` (graph có 1 task đỏ), `crash_2_red_task_log.png` (log `silver_frames` map_index 17, thấy `trip-06570`), `crash_3_graph_green_after_clear.png` (graph xanh sau khi clear).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt mình thấy ở `lake.bronze_raw_telemetry_a` và `lake.bronze_raw_telemetry_b` (đầu ra `step 1`):

1. **Tên cột khác hẳn**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (dòng đầu `1.213`), B là km/h (dòng đầu `55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO-8601 UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`).
4. **Bàn đạp**: A là tỷ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: `bronze.video_files` ghi `telemetry_clock_offset_ms` = 0 cho drive A nhưng **1500** cho drive B (thiết bị B chạy nhanh hơn video 1,5 s).
6. Số dòng khác nhau: 3021 (A) và 3234 (B), vì drive B dài hơn (321,4 s so với 300,1 s).

Vì sao **mỗi nguồn một bảng** và **mọi cột là chữ**: bronze là bản sao nguyên trạng, chỉ thêm. Gộp hai nguồn thì phải chọn một schema, tức là đã biến đổi dữ liệu ngay ở bronze. Nếu luật đổi đơn vị hay độ lệch bị sai thì ta mất bản gốc để sửa. Giữ dạng chữ thì việc nạp không bao giờ hỏng vì một ô lạ: `typeof(speed_mps)` là `VARCHAR`, và 30 ô tốc độ rỗng vẫn là `''` chứ không thành `NaN`/0. Mọi việc ép kiểu dồn về dbt staging, nơi có thể chạy lại bất cứ lúc nào từ bronze. Các cột lineage `_batch_id` (`17f098d6bbb1` = 12 ký tự đầu sha256 của file A), `_row_no`, `_source_file` cho phép truy một dòng silver về đúng dòng CSV. Mình cũng kiểm tra tính idempotent: lần nạp thứ hai báo `already present`, và `cli history bronze.raw_telemetry_a` chỉ có **một commit** (version 0, 3021 dòng).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Chuẩn hoá nằm ở hai view staging. `stg_telemetry_a` chỉ ép kiểu, vì A đã là m/s, ISO và không lệch đồng hồ. `stg_telemetry_b` đổi tên cột về đúng tên của A, đổi `spd_kmh / 3.6` → m/s, đổi bàn đạp `% / 100` → 0..1 và đổi thời gian bằng `to_timestamp((timestamp_ms - offset_ms) / 1000)`. Trong đó `offset_ms = 1500` lấy từ `bronze.video_files`, tức từ `sources.json` lúc ingest. Sau đó `silver_telemetry_classified` gộp hai nguồn (`union all by name`) và gán cho từng dòng một `reject_reason`.

Bằng chứng ở bước 2 của mình:
- B bắt đầu lúc `2025-10-08 14:30:00.009` (cột `bat_dau`), khớp giờ bắt đầu video `14:30:00` (lệch 9 ms). Nếu chưa trừ 1500 ms thì giá trị này sẽ là `14:30:01.509`.
- Tốc độ tối đa sau chuẩn hoá: A 71 km/h, B 100 km/h. Cả hai đều hợp lý, nghĩa là B đã được chia 3,6 đúng (không còn ra 360 km/h).
- verify module 2: **7/7**, trong đó có `2968+53=3021` (A) và `3181+53=3234` (B).

| drive | duplicate | speed_missing | speed_out_of_range | sạch |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 |
| drive_b | 20 | 30 | 3 | 3181 |

Dòng lỗi vào `silver_telemetry_rejects` kèm lý do, `_batch_id` và `_row_no` thay vì bị xoá. Lý do thứ nhất là kiểm toán được: luôn có `sạch + cách ly = nhận về`, nên biết chắc không dòng nào biến mất âm thầm. Thứ hai là có thể sửa và chạy lại: nếu luật sai (ví dụ ngưỡng tốc độ) thì vẫn còn dòng gốc để phục hồi. Thứ ba là thấy được lỗi của nguồn: 3 dòng `250 m/s` ở `_row_no` 400/1700/2500 của cả hai drive là lỗi cảm biến lặp lại cần báo cho đội phần cứng.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`in_range` (tốc độ 0..70 m/s) là `error` vì nó bảo vệ một **bất biến**. Một tốc độ 250 m/s lọt vào silver thì sẽ đi thẳng vào nhãn huấn luyện (`speed_mps` có trong gold), làm hỏng mô hình một cách âm thầm, nên phải dừng pipeline. `max_gap_ms` là `warn` vì nó đo **độ đầy đủ**. Cảm biến mất tín hiệu là chuyện có thật và đã biết; dữ liệu còn lại vẫn đúng, chỉ thưa hơn, nên báo để người vận hành biết nhưng không chặn. Ở lần chạy của mình test này WARN với đúng 2 khoảng hở: `drive_a` 3073 ms (tới `08:02:02.189`) và `drive_b` 3124 ms (tới `14:31:32.712`). Hai khoảng này trùng với 30 dòng `speed_missing` đã bị cách ly (A: 08:01:59.2→08:02:02.1). Vậy chính quarantine tạo ra lỗ hổng, và test warn báo lại điều đó.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` lên 1000 thì `dbt test` cho `PASS=5 WARN=1 ERROR=1`. Test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng**, đúng 3 + 3 dòng 250 m/s mà luật làm sạch đã bị nới cho lọt qua. Khôi phục luật thì trở lại `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là **lớp phòng thủ thứ hai, độc lập** với luật làm sạch: luật biến đổi có thể bị sửa sai (do người hoặc do một biến `--vars`), nhưng test viết riêng trong `schema.yml` vẫn chặn. Trong DAG, `dbt_test_silver` đỏ thì `dbt_run_gold` và các task sau không chạy, nên dữ liệu xấu không tới gold hay warehouse.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Mình chạy `exp idempotency --trip trip-06570` (trip của mình). Kết quả trong `idempotency.json`:
- Cách của pipeline (Delta `overwrite` với `predicate trip_id = 'trip-06570'`): **trước 1246 dòng, dấu vân tay `6c7480dfde7e`**. Sau **hai lần** chạy lại partition: **vẫn 1246 dòng, vẫn `6c7480dfde7e`**, `unchanged: true`.
- Cách sai (`append` hai lần, chỉ trip này): trip có 45 khung nhưng bảng thử có **90 dòng**, tức nhân đôi (2x).
- `cli history silver.frames`: version 26 và 27 đều là `WRITE` với `predicate trip_id = 'trip-06570'` và 45 dòng. Mỗi lần chạy lại là một commit **thay thế** 45 dòng đó, không cộng thêm.

Vì sao `overwrite` theo partition an toàn hơn "xoá rồi ghi lại": với Delta, xoá các file cũ của trip và thêm file mới nằm trong **một commit nguyên tử** vào `_delta_log`. Người đọc chỉ thấy phiên bản trước hoặc phiên bản sau, không bao giờ thấy trạng thái nửa vời. Nếu làm hai bước `DELETE` rồi `INSERT` mà job sập ở giữa (đúng kịch bản `LAB_FAIL_ONCE`), bảng sẽ **mất** trip đó cho tới khi có người sửa, và người đọc ở giữa thấy dữ liệu thiếu. Nếu sập trong lúc `INSERT` nhiều file thì có thể còn ghi một phần. Với overwrite có predicate, sập trước khi commit nghĩa là bảng vẫn y như cũ; chạy lại thì cho đúng kết quả cũ. Ngoài ra predicate giới hạn phạm vi ghi trong **một** trip, nên chạy lại `trip-06570` không động tới 25 trip còn lại, và phiên bản cũ vẫn còn để time travel.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(tổng: A 602 khung, B 644 khung; `threshold.json`)

**Khung đen** là ảnh gần như không có tín hiệu (`brightness < 12`). Ở lần chạy của mình 6 khung đen của A và 4 khung đen của B đều có độ sáng **0.0**. Đó là camera hỏng hay bị che, không mang thông tin, nên bị **loại** khỏi gold. **Khung tối** (`12 ≤ brightness < 40`) vẫn có cảnh thật: B có **61** khung tối (độ sáng 12,8 – 39,6, trung bình 30,8), tập trung ở `trip-03365` (25 khung) và `trip-05297` (16 khung), là đoạn hầm/bóng râm được cấy ở giây 140–145. Khung tối chỉ được **gắn cờ `is_dark` và giữ lại**. Mình đồng ý: xe tự lái phải chạy được cả trong hầm và ban đêm, bỏ khung tối thì dataset sẽ thiên về điều kiện đẹp. Cờ `is_dark` vẫn cho phép lọc hoặc cân bằng lại sau này. Có một điểm cần lưu ý: luật `is_blurry` bỏ qua khung đen nhưng **không** bỏ qua khung tối, nên khung tối mà ít cạnh có thể bị loại nhầm vì "mờ".

Mình chọn **giữ ngưỡng 2**. Ngưỡng 0 chỉ bắt ảnh trùng tuyệt đối. Ngưỡng 6 coi 60% khung của A (363/602) là "trùng", rõ ràng là loại cả những khung khác nhau thật. Ngưỡng 2 bắt được đoạn đứng hình (11/12 khung trong verify) mà vẫn giữ phần lớn dữ liệu. Ngưỡng này **phụ thuộc tốc độ**: ở 0–10 km/h có **50,9%** khung bị coi là trùng (55/108), vì xe gần như đứng yên nên thật sự dư thừa. Mình bất ngờ khi thấy ở ≥ 50 km/h tỷ lệ vẫn là 13,7% (104 khung, không khung nào nằm trong đoạn đứng hình), cao hơn ở 10–50 km/h (~9–10%). Mình mở thử cặp `drive_a-02406` / `drive_a-02411` (65 km/h, dHash khác **0 bit**): hai ảnh **khác nhau thật**, hàng rào và hàng cây bên phải đã dịch chuyển vì xe đi khoảng 9 m. Chỉ là bố cục thô (đường thẳng, xe phía trước giữ khoảng cách) gần như không đổi. dHash 9×8 không phân biệt được "camera đứng hình" với "cảnh cao tốc ít thay đổi". Vì vậy ngưỡng hợp lý nên đi theo tốc độ: ngưỡng 2 (hoặc cao hơn) khi xe chậm hay đứng, ngưỡng 0–1 khi xe chạy nhanh.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` của mình: có **1019** khung đáng dùng thuộc **26** trip. Khi chia **theo khung hình** (băm `frame_id`, 70/15/15), **26/26 trip** bị xé ra nhiều split và **26/26 trip có khung ở cả train lẫn test**. Khi chia **theo trip** (gold của pipeline) thì **0** trip bị chia cắt, và test dbt `no_straddle` cũng PASS.

Mỗi trip trung bình khoảng 39 khung, mà chia ngẫu nhiên theo khung thì gần như chắc chắn trip nào cũng có khung rơi vào test. Hai khung liền nhau chỉ cách 0,5 s, gần như cùng một cảnh (cùng đường, cùng xe phía trước, cùng ánh sáng). Nếu khung t nằm ở train và khung t+0,5 s nằm ở test, mô hình chỉ cần "nhớ" cảnh là đoán đúng. Điểm test khi đó đo khả năng **ghi nhớ** chứ không đo khả năng **tổng quát hoá** sang một con đường mới. Điểm cao giả tạo nên ta tưởng mô hình tốt, rồi nó hỏng khi lên xe thật. Chia theo trip (theo cảnh) buộc test là những đoạn đường mô hình **chưa từng thấy**. Ở split của mình, test là `trip-07774`, `trip-09608` (drive A) và `trip-01037`, `trip-04324` (drive B).

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version = sha256` của danh sách `frame_id|split` đã sắp xếp, lấy 12 ký tự đầu. Nó là **hàm của nội dung**: cùng tập khung và cùng cách chia thì ra cùng version, khác dù chỉ một khung hay một split thì ra version khác. Ngày giờ thì đổi mỗi lần chạy dù dữ liệu y hệt, còn số đếm tăng dần thì phụ thuộc vào việc ai chạy bao nhiêu lần. Cả hai đều không trả lời được câu "hai mô hình này có được huấn luyện trên cùng dữ liệu không?". Băm nội dung thì trả lời được, chạy lại ở máy khác vẫn ra cùng số, và nó cũng là tên thư mục `gold/datasets/<version>/` nên ghi lại không bao giờ đè lên một dataset khác.

Kết quả của mình: `step all --reset` lần đầu cho **`7a789a787ab8`**; chạy lại `step 4` và `cli export-dataset` vẫn ra **`7a789a787ab8`** (cùng `manifest_sha256 4db243f2…`). Khi mình đổi MSSV sang `2A202602782` và chạy lại `step 4`, version thành **`1888ff343d4d`**, split thành 675/167/177 khung (với MSSV của mình là 643/215/161). Đặt lại `2A202602781` thì version quay về đúng **`7a789a787ab8`**. Lý do là `gold_dataset_frames` xếp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV là "muối" của phép xáo trộn. Cùng dữ liệu, cùng luật lọc (vẫn 1019 khung, vẫn 18/4/4 trip), nhưng **trip nào vào train/val/test** thì khác, nên danh sách `frame_id|split` khác và hash khác. Mỗi bạn có một version riêng, và giảng viên chạy lại với MSSV của mình sẽ ra đúng `7a789a787ab8`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Phần `lineage` trong `gold/datasets/7a789a787ab8/manifest.json` của mình (lần chạy cuối `step all --reset`):

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
  "silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" } }
```

Đọc ra: dataset này gồm 1019 khung (436 từ drive A, 583 từ drive B). Nó được tạo từ **hai file video có đúng hai sha256 trên** (trùng sha256 trong `data/MANIFEST.json`) và từ telemetry bronze ở version 0 của mỗi nguồn. Khung hình lấy từ `silver.frames` **version 25**, tức đúng 26 lần ghi theo trip (version 0–25). Sau thí nghiệm idempotent, manifest cũ ghi `silver.frames: 27` (thêm 2 lần chạy lại `trip-06570`), nhưng `dataset_version` và `manifest_sha256 4db243f2…` không đổi: version của bảng tăng, còn nội dung thì giống hệt. Cờ và join lấy từ `silver.frames_flagged`/`frame_telemetry` version 0. Luật lọc nằm trong `rules`: bỏ `is_black, is_blurry, is_near_dup, not sync_ok`, chia theo trip, phân tầng theo drive, thứ tự md5.

Nếu ba tháng sau có người hỏi, để tái tạo đúng dataset cần: (1) hai video theo sha256 và các CSV gốc (bronze giữ nguyên, có `_batch_id`); (2) các version Delta ở trên, đọc bằng time travel (`DeltaTable(..., version=27)`) kể cả khi bảng đã được ghi tiếp; (3) commit git của mã `dbt/` (ngưỡng trong `dbt_project.yml`: `dark_luma 12`, `blur_var 40`, `dup_hamming 2`, `sync_tol_ms 150`, `split 0.70/0.15`); (4) **MSSV `2A202602781`** dùng làm muối khi chia. Cuối cùng, so `dataset_version` và `manifest_sha256` để chứng minh đã tái tạo đúng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của mình là `202602781 % 26 = 17` → **`trip-06570`**. MSSV `2A202602781` có chữ `A` nên lệnh `int(MSSV)` trong GUIDE báo lỗi; mình dùng phần số `202602781`. Các bước: đặt `LAB_FAIL_ONCE=trip-06570`, `docker compose up -d`, trigger `day09_reset` (run `reset_before_crash`), rồi trigger `day09_video_pipeline` (run `crash_run`).

- **Đúng 1 task đỏ**: `silver_frames` **map_index 17**. Log có dòng `RuntimeError: simulated crash while extracting trip-06570; run it again` (ảnh `crash_2_red_task_log.png`).
- **7 task `upstream_failed`**: `dbt_run_silver, dbt_test_silver, dbt_run_gold, dbt_test_gold, publish_lake, export_dataset, publish_warehouse`. Còn **29 task xanh**: 3 bronze, `trip_commands` và 25 trip khác, vì mỗi trip là một task độc lập (ảnh `crash_1_graph_red.png`).
- Mình **chỉ clear task đỏ** (kèm downstream của nó, chỉ các task failed/upstream_failed). Kết quả: chỉ `silver_frames/map_index=17` có `attempt=2`, 7 task phía sau chạy lần đầu, **25 trip xanh không chạy lại**. Tổng cộng 8 task chạy sau khi clear, và DAG chuyển **xanh 37/37** (ảnh `crash_3_graph_green_after_clear.png`).
- **Không nhân đôi**: `lake.silver_frames` có **1246 dòng / 1246 `frame_id` khác nhau**, trong đó `trip-06570` có 45 dòng (đúng bằng lúc chạy bình thường). `history silver.frames` chỉ có 26 lần ghi (version 0–25): lần chạy bị sập **không để lại commit nào**, vì crash xảy ra trước khi ghi, và lần chạy lại thì overwrite đúng partition của trip đó.
- `scripts/lab.sh verify` sau đó: **27/27**, `dataset_version` **vẫn là `7a789a787ab8`** (không đổi, đúng như kỳ vọng vì nội dung giống hệt).

Xong, mình xoá dòng `LAB_FAIL_ONCE` khỏi `.env` và chạy lại `docker compose --profile orchestrate up -d`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Mình ước lượng bằng số của chính lab: 26 trip (621,5 s video) được tách khung trong khoảng **123 s** ở `full_run_1`, tức khoảng 5 lần nhanh hơn thời gian thực, chạy tuần tự từng trip. 500 xe × khoảng 8 giờ/ngày là khoảng 14,4 triệu giây video/ngày, nên với kiến trúc này cần khoảng **33 ngày máy cho mỗi ngày dữ liệu**. Ba thành phần sẽ vỡ trước:

1. **Bước tách khung (`frames.extract_trip`)**. Mỗi trip tải **cả video của drive** về (26 lần cho mỗi drive), giải mã bằng một FFmpeg CPU trong **một** container Airflow (`LocalExecutor`, `parallelism 2`, `max_active_tis_per_dagrun=1`), rồi `put_object` từng JPG nhỏ (≈ 29 triệu object mỗi ngày ở 2 khung/giây). Mình sẽ thay bằng: cắt video thành segment ngay lúc ingest; giải mã phân tán trên một cụm worker (Ray/Spark hoặc job Kubernetes, có thể dùng GPU NVDEC); ghi khung thành shard lớn (WebDataset/tar hoặc Parquet chứa ảnh) thay vì hàng triệu file nhỏ. Đây đúng là nút cổ chai "giải mã video" ở slide 24; video vẫn nằm ở object storage và bảng chỉ giữ con trỏ (slide 17).
2. **Lớp biến đổi DuckDB + dbt**. DuckDB là **một file, một tiến trình ghi** (`threads: 1`). Mọi model silver/gold là `table` được build lại toàn bộ, và `publish_lake` **overwrite cả bảng** mỗi lần chạy, nên chi phí tăng theo toàn bộ lịch sử chứ không theo dữ liệu mới. Mình sẽ thay bằng model **incremental** theo partition (ngày, xe) với `MERGE` vào Delta, chạy trên engine phân tán (Spark, Trino hoặc Databricks SQL), tức phần ELT và kiểm tra chất lượng bằng dbt ở slide 22, nhưng trên engine chịu được quy mô lớn.
3. **Điều phối và ingest**. Một mapped task cho mỗi trip, nhân với 500 xe, sẽ thành hàng chục nghìn task mỗi lần chạy, quá tải scheduler và metadata DB Postgres dùng chung. Ingest thì kiểm tra trùng bằng cách đọc **toàn bộ** cột `_batch_id` (tăng tuyến tính), và đọc CSV bằng Python. Mình sẽ thay bằng ingest theo sự kiện (xe upload xong thì đẩy message qua hàng đợi Kafka/SQS), điều phối và backfill theo partition ngày–xe (slide 26–27) với `CeleryExecutor`/`KubernetesExecutor`, và kiểm tra idempotent bằng bảng chỉ mục sha256 có khoá (hoặc `MERGE` theo `_batch_id`). MinIO một node cũng phải lên S3 hoặc cụm phân tán.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi 1: tỷ lệ khung bị loại theo từng trip** (card "Câu hỏi riêng: tỷ lệ khung bị loại theo từng trip", ảnh `metabase_dashboard.png`):

```sql
select trip_id, drive_id, split, lighting, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped, avg_speed_kmh
from marts.mart_trip_summary
order by pct_dropped desc
```

Bốn trip bị loại nhiều nhất đều thuộc drive A: `trip-04622` 56,7% (26/60 khung giữ lại, 67 km/h), `trip-03904` 50%, `trip-02086` 46,7%, `trip-08888` 40%. Mình tra lý do trong `silver.silver_frame_telemetry`: **toàn bộ** số khung bị loại của bốn trip này là `is_near_dup` (không có khung đen, mờ hay mất đồng bộ). `trip-03904` (giây 147–158) chứa đoạn camera đứng hình được cấy (150–156 s), và `trip-08888` chạy 6 km/h, nên hai trường hợp này là trùng thật. Nhưng `trip-04622` (giây 240–270, **67 km/h**, không có lỗi cấy nào) mất 34 khung vì dHash coi là trùng, dù mình mở ảnh ra thấy xe vẫn đang di chuyển (cảnh vật bên đường dịch chuyển giữa hai khung). Đây là điều dashboard giúp mình phát hiện: ngưỡng dHash cố định gộp chung "đứng hình" với "cao tốc ít thay đổi", nên dataset bị thiếu dữ liệu cao tốc (xem thêm câu 5).

**Câu hỏi 2 (vận hành): tập test có cùng phân bố tốc độ với train không?**

```sql
select drive_id, split, count(*) as frames, round(avg(speed_kmh)::numeric, 1) as avg_kmh,
       round(100.0 * avg(case when speed_kmh < 10 then 1 else 0 end)::numeric, 1) as pct_under_10kmh
from marts.mart_frame_gallery
group by 1, 2
order by 1, case split when 'train' then 1 when 'val' then 2 else 3 end
```

Kết quả: ở drive A, test có trung bình **11,9 km/h** và **40,4%** khung dưới 10 km/h, trong khi val là 64,1 km/h với 0% khung chậm, còn train là 36,6 km/h. Drive B thì test (86 km/h) nhanh hơn train (65,9 km/h). Chia theo trip là đúng để chống rò rỉ, nhưng với chỉ 2 trip mỗi split, **phân bố tốc độ lệch mạnh**: điểm test của drive A chủ yếu đo hành vi chạy chậm. Muốn split đại diện hơn thì nên phân tầng thêm theo dải tốc độ hoặc ánh sáng khi xếp trip (vẫn giữ nguyên tắc một trip một split).

## Thí nghiệm riêng (điểm cộng): độ nhạy của `sync_tol_ms`

Mình chạy lại `silver_frame_telemetry` và `gold_dataset_frames` với `--vars '{sync_tol_ms: X}'` (không sửa file `dbt_project.yml`, chạy xong thì build lại với giá trị gốc 150, không publish). Bằng chứng: `report/evidence/own_experiment_sync_tol.json`.

| `sync_tol_ms` | khung `sync_lost` | % | khung gold | train / val / test | gap lớn nhất trong gold | dbt test |
|---|---|---|---|---|---|---|
| 50 | 600 | 48,15% | 538 | 352 / 98 / 88 | 20 ms | WARN `sync_lost` > 5% |
| 100 | 324 | 26,0% | 762 | 489 / 150 / 123 | 100 ms | WARN `sync_lost` > 5% |
| **150** (gốc) | 14 | 1,12% | **1019** | 643 / 215 / 161 | 120 ms | PASS |
| 300 | 14 | 1,12% | 1019 | 643 / 215 / 161 | 120 ms | PASS |
| 1000 | 11 | 0,88% | 1022 | 646 / 215 / 161 | **884 ms** | PASS |

Giải thích: telemetry lấy mẫu với chu kỳ trung vị **100 ms** (p95 khoảng 127 ms) ở cả hai drive, nên khi join ASOF thì khoảng cách khung–telemetry nằm trong 0..~128 ms. Ngưỡng dưới chu kỳ lấy mẫu (50, 100 ms) loại oan gần nửa dataset, và test `max_share` của `sync_lost` đã cảnh báo đúng. Từ 150 đến 300 ms kết quả không đổi (vùng ổn định). Nới tới 1000 ms chỉ thêm 3 khung, nhưng đó là các khung nằm trong đoạn cảm biến mất tín hiệu, mang tốc độ "cũ" tới 884 ms. Với xe 70 km/h, 884 ms là khoảng 17 m, tức nhãn sai. Vì vậy 150 ms là lựa chọn đúng: vừa trên p95 của chu kỳ lấy mẫu, vừa đủ chặt để chặn nhãn cũ. Nếu đổi ngưỡng thì `dataset_version` cũng đổi (1022 khung thay vì 1019), đúng như thiết kế băm nội dung.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: sập rồi clear chỉ chạy lại 1 trip + 7 task sau, không nhân đôi dòng nào, `dataset_version` vẫn `7a789a787ab8` (4 lần chạy lại từ lake trống đều ra đúng số này).
- **Pipeline loại nhầm**: dHash 9×8, ngưỡng 2 coi là "trùng" cả khung cao tốc thật (`trip-04622` mất 34/60 khung ở 67 km/h), vì bố cục thô gần như không đổi dù xe đã đi khoảng 9 m.
- **Lỗi chưa được kiểm**: 15 dòng telemetry đến sai thứ tự không có luật hay test nào đếm; dbt chỉ `order by ts` nên chúng bị sửa âm thầm.
- **Còn chưa rõ**: vì sao join lấy telemetry gần nhất *trước* khung (ASOF) thay vì gần nhất hai phía, nên luôn trễ trung bình 66–73 ms.
- **Môi trường**: trên Windows mình phải chuyển `.sh` về LF và chạy `lab.sh` với `MSYS_NO_PATHCONV=1`; Claude hỗ trợ dò lỗi môi trường, kiểm tra số liệu và chạy thử.
