import requests
import statistics
import json

API_URL = "http://localhost:8000/user/docs/search"

queries = [
    # Nhóm 1: rất ngắn
    "internet",
    "byte",
    "bit",
    "dữ liệu",
    "thuật toán",
    "mạng",
    "ip",
    "máy tính",
    "phần mềm",
    "phần cứng",
    "bảng tính",
    "tệp tin",
    "thư mục",
    "hệ điều hành",
    "trình duyệt",
    "thông tin",
    "an toàn mạng",
    "địa chỉ ip",
    "mạng máy tính",
    "thiết bị số",
    "dữ liệu số",
    "lưu trữ",
    "kết nối",
    "virus máy tính",
    "bảo mật",

    # Nhóm 2: ngắn – trung bình
    "internet là gì",
    "byte là gì",
    "bit là gì",
    "dữ liệu là gì",
    "thuật toán là gì",
    "mạng máy tính là gì",
    "địa chỉ ip là gì",
    "phần mềm là gì",
    "phần cứng là gì",
    "bảng tính là gì",
    "hệ điều hành là gì",
    "trình duyệt web là gì",
    "dữ liệu số là gì",
    "tệp và thư mục",
    "kết nối internet",
    "an toàn thông tin mạng",
    "virus máy tính là gì",
    "thiết bị vào ra",
    "các loại phần mềm",
    "cách lưu trữ dữ liệu",
    "máy tính hoạt động ra sao",
    "thông tin trong máy tính",
    "biểu diễn dữ liệu trong máy tính",
    "khái niệm mạng internet",
    "thiết bị mạng cơ bản",

    # Nhóm 3: mô tả trung bình
    "cho tôi tài liệu về internet",
    "tìm tài liệu về byte",
    "tìm hiểu về bit trong máy tính",
    "tài liệu về dữ liệu số",
    "tài liệu về mạng máy tính",
    "tài liệu về địa chỉ ip",
    "tài liệu về phần mềm máy tính",
    "tài liệu về phần cứng máy tính",
    "tài liệu về hệ điều hành",
    "tài liệu về bảng tính",
    "cách kết nối internet là gì",
    "mạng máy tính hoạt động như thế nào",
    "dữ liệu trong máy tính được lưu như thế nào",
    "sự khác nhau giữa byte và bit",
    "các thành phần của máy tính",
    "tìm hiểu về an toàn thông tin mạng",
    "vai trò của hệ điều hành",
    "chức năng của phần mềm máy tính",
    "cách tổ chức tệp và thư mục",
    "máy tính xử lý dữ liệu như thế nào",
    "tìm hiểu về virus máy tính",
    "địa chỉ ip dùng để làm gì",
    "internet có vai trò gì",
    "thiết bị mạng gồm những gì",
    "bảng tính dùng để làm gì",

    # Nhóm 4: dài / nhiều ý
    "cho tôi tài liệu về internet và mạng máy tính",
    "tìm hiểu về byte và bit trong máy tính",
    "dữ liệu trong máy tính được biểu diễn như thế nào",
    "sự khác nhau giữa phần mềm và phần cứng là gì",
    "vai trò của internet trong học tập và đời sống",
    "cách hoạt động của mạng máy tính là gì",
    "cho tôi tài liệu về an toàn khi sử dụng internet",
    "các thành phần của hệ thống máy tính gồm những gì",
    "địa chỉ ip có vai trò gì trong mạng máy tính",
    "hệ điều hành giúp máy tính hoạt động như thế nào",
    "cách lưu trữ và quản lý dữ liệu trong máy tính",
    "bảng tính được sử dụng trong những trường hợp nào",
    "virus máy tính ảnh hưởng đến hệ thống ra sao",
    "cho tôi tài liệu về thiết bị vào và thiết bị ra",
    "sự khác nhau giữa dữ liệu và thông tin là gì",
    "tìm tài liệu về các loại phần mềm máy tính",
    "máy tính xử lý dữ liệu đầu vào như thế nào",
    "mạng internet mang lại những lợi ích gì",
    "cách tổ chức tệp và thư mục trên máy tính",
    "vì sao cần đảm bảo an toàn thông tin mạng",
    "cho tôi tài liệu nói về hệ điều hành và phần mềm",
    "sự khác nhau giữa mạng internet và mạng máy tính",
    "dữ liệu số có vai trò gì trong máy tính",
    "cho tôi tài liệu về thiết bị mạng cơ bản",
    "thuật toán được ứng dụng trong tin học như thế nào",
]

results = []

print("=== BAT DAU BENCHMARK SEARCH ===\n")

for index, query in enumerate(queries, start=1):
    try:
        response = requests.get(
            API_URL,
            params={
                "q": query,
                "category": "all",
                "limit": 10,
                "offset": 0
            },
            timeout=60
        )

        response.raise_for_status()
        data = response.json()

        search_time_ms = data.get("searchTimeMs", None)
        total_result = data.get("total", 0)

        results.append({
            "stt": index,
            "query": query,
            "searchTimeMs": search_time_ms,
            "total": total_result
        })

        print(f"{index:03d}. {query}")
        print(f"      -> time: {search_time_ms} ms | total: {total_result}")

    except Exception as e:
        results.append({
            "stt": index,
            "query": query,
            "searchTimeMs": None,
            "total": None,
            "error": str(e)
        })

        print(f"{index:03d}. {query}")
        print(f"      -> ERROR: {e}")

valid_times = [
    r["searchTimeMs"]
    for r in results
    if isinstance(r.get("searchTimeMs"), (int, float))
]

print("\n=== KET QUA TONG HOP ===")

if valid_times:
    print(f"So query thanh cong: {len(valid_times)}/{len(queries)}")
    print(f"Thoi gian trung binh: {statistics.mean(valid_times):.2f} ms")
    print(f"Nho nhat: {min(valid_times):.2f} ms")
    print(f"Lon nhat: {max(valid_times):.2f} ms")
else:
    print("Khong co query nao lay duoc searchTimeMs")

# Chia nhóm
group_1_times = [r["searchTimeMs"] for r in results[0:25] if isinstance(r.get("searchTimeMs"), (int, float))]
group_2_times = [r["searchTimeMs"] for r in results[25:50] if isinstance(r.get("searchTimeMs"), (int, float))]
group_3_times = [r["searchTimeMs"] for r in results[50:75] if isinstance(r.get("searchTimeMs"), (int, float))]
group_4_times = [r["searchTimeMs"] for r in results[75:100] if isinstance(r.get("searchTimeMs"), (int, float))]

print("\n=== THEO NHOM QUERY ===")

if group_1_times:
    print(f"Nhom 1 - Rat ngan TB: {statistics.mean(group_1_times):.2f} ms")
if group_2_times:
    print(f"Nhom 2 - Ngan/trung binh TB: {statistics.mean(group_2_times):.2f} ms")
if group_3_times:
    print(f"Nhom 3 - Mo ta trung binh TB: {statistics.mean(group_3_times):.2f} ms")
if group_4_times:
    print(f"Nhom 4 - Dai/nhieu y TB: {statistics.mean(group_4_times):.2f} ms")

# Lưu file JSON
with open("benchmark_search_results_100.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("\nDa luu ket qua vao file: benchmark_search_results_100.json")