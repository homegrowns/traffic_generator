import tkinter as tk
import subprocess
from tkinter import ttk 
import re
from etl_to_traffic import *
from threading import Thread

root = tk.Tk()
root.title("traffic_generator")
root.geometry("900x400")

    
running = False
complete = False
user_attack_input = ""
user_ip_input = ""

total_packet = 10 #15000 # 여기서 리터럴로 개수설정 아차피 시험 절차서에 명시된 개수로 해야된다.
# 1초 천개 이상합격
# 대규모 네트워크 환경에서 수집 된 데이터를 시스템이, 
# 15분간 처리하는 데이터양을 확인 100만개 이상 이면 합격

count = 0

placeholder_ip = "IP 주소를 입력하세요"
placeholder_attack = "공격 방식을 입력하세요"
placeholder_num = "전송할 데이터수를 입력하세요"

# 정규식 기반: 입력 중에도 허용 가능한 IP 문자열 검사
def validate_ip_partial(text):
    # placeholder 허용
    if text == "" or text == placeholder_ip:
        return True

    # 숫자와 점(.)으로만 구성되어야 함
    if not all(c.isdigit() or c == '.' for c in text):
        return False

    # IP 형식 일부라도 맞아야 함 (예: 192., 192.168, 등)
    pattern = r"^(\d{1,3}\.?){0,3}\d{0,3}$"
    return re.match(pattern, text) is not None


vcmd = (root.register(validate_ip_partial), "%P")

# --- 위젯들 미리 선언 (한 번만 생성) ---


target_input = tk.Entry(root, font=("Helvetica", 16),
                        validate="key", 
                        validatecommand=vcmd, 
                        fg='grey')
                       
target_input.insert(0, placeholder_ip)
target_input.bind('<FocusIn>', lambda e: (target_input.delete(0, 'end'), target_input.config(fg='black')) if target_input.get() == placeholder_ip else None)
target_input.bind('<FocusOut>', lambda e: target_input.insert(0, placeholder_ip) if target_input.get() == '' else None)

attack_input = tk.Entry(root, font=("Helvetica", 16), fg='grey')
attack_input.insert(0, placeholder_attack)
attack_input.bind('<FocusIn>', lambda e: (attack_input.delete(0, 'end'), attack_input.config(fg='black')) if attack_input.get() == placeholder_attack else None)
attack_input.bind('<FocusOut>', lambda e: attack_input.insert(0, placeholder_attack) if attack_input.get() == '' else None)

amount_input = tk.Entry(root, font=("Helvetica", 16),
                        validate="key", 
                        fg='grey')
amount_input.insert(0, placeholder_num)
amount_input.bind('<FocusIn>', lambda e: (amount_input.delete(0, 'end'), amount_input.config(fg='black')) if amount_input.get() == placeholder_num else None)
amount_input.bind('<FocusOut>', lambda e: amount_input.insert(0, placeholder_num) if amount_input.get() == '' else None)

target_ip = tk.Label(root, text="", font=("Helvetica", 16))
attack_name = tk.Label(root, text="", font=("Helvetica", 16))

packet_num = tk.Label(root, text="패킷 0개", font=("Helvetica", 20))
progress = ttk.Progressbar(root, orient="horizontal", length=250, mode="determinate")
packet_status = tk.Label(root, text="", font=("Helvetica", 8))
# btn_pogr = tk.Button(root, text="시작")
btn_ip_show = tk.Button(root, text="확인")
# 버튼 구성
start_btn = tk.Button(root, text="▶ 시작")
stop_btn = tk.Button(root, text="⏸ 중지")
btn_refresh = tk.Button(root, text="재시작")
# =======================================================================================




# --- 함수: IP 입력 후 진행창 보여주기 ---
def ip_input():
    global etl, total_packet
    user_ip_input = target_input.get()
    user_attack_input = attack_input.get()
    total_packet = int(amount_input.get())
    target_ip.config(text=f"Target IP: {user_ip_input}")
    attack_name.config(text=f"Attack Name: {user_attack_input}")

    # 패킷 전송 함수 호출
    etl = ClickHouseETL(host='211.115.206.7', tg_ip=user_ip_input, attack=user_attack_input, limit=total_packet)
    etl.extract()
    
    # 입력창 숨기기
    target_input.pack_forget()
    btn_ip_show.pack_forget()
    attack_input.pack_forget()
    amount_input.pack_forget()

    # 진행화면 보여주기
    target_ip.pack()
    attack_name.pack()
    packet_num.pack(pady=40)
    progress.pack()
    packet_status.pack(pady=10)

    # --- 버튼 배치 ---
    start_btn.pack(side="left", padx=20)
    stop_btn.pack(side="right", padx=20)   
    

# --- 진행 업데이트 함수 ---
def update_progress(count: int, result: str, error: int):
    global running, total_packet, complete, user_ip_input
    failed = "" 
    # 실패한 패킷 수를 세는 로직
    if result.startswith("[!]"):
        failed = result
        print(f"전송 실패 패킷수= {error}")
        packet_num.config(text=f"패킷 {total_packet} 개중 {error}개 전송 실패")
        packet_status.config(text=failed)
        packet_status.config(fg="red")
    # 리턴 값으로  전송될 전체 패킷 개수
    elif running and count < total_packet:
        print(f"전송 시도 패킷수= {count}")
        packet_num.config(text=f"패킷 {total_packet} 개중 {count} 전송중")
        packet_status.config(text=result)
        packet_status.config(fg="green")
        
        progress['value'] = count

        progress.update_idletasks()
    elif count == total_packet:
        complete = True
        progress['value'] = progress['maximum']
        progress.update_idletasks()
        running = False
        success = count - error
        packet_num.config(text=f"[결과] 패킷 {total_packet} 개중 {success} 전송성공 {error}개 전송실패")
        packet_status.config(text=failed)
        packet_status.config(fg="red")
    

# --- 시작 버튼 ---
def start_progress():
    global running, complete
    Thread(target=lambda: etl.send_requests(progress_callback=update_progress)).start()
    complete = False
    running = True

# --- 중지 버튼 ---
def stop_progress():
    global running, complete
    running = False
    if complete:
        packet_num.config(text=f'현재 {total_packet}개 전송 완료')
    elif not complete and running:    
        packet_num.config(text=f'⏹️ 중지됨 현재 {total_packet}중 패킷 {count}개 전송')
    else:
        packet_num.config(text=f'⏹️ 중지됨 현재 {total_packet}중 패킷 {count}개 전송')
        
    etl.send_requests(progress_callback=update_progress, stop=True)

# --- 버튼 과 함수 매칭 ---

start_btn.config(command=start_progress)

stop_btn.config(command=stop_progress)


# --- 함수: 재시작 (초기 UI로 되돌리기) ---
def refresh():
    global count, user_attack_input, user_ip_input, total_packet
    user_attack_input = ""
    user_ip_input = ""
    total_packet = 0
    count = 0
    root.after(200, stop_progress())

    # 진행 바 초기화
    progress['value'] = 0
    packet_num.config(text=f'패킷 {count}개 전송 완료 (초기화됨)')
    # 진행 화면 숨기기
    target_ip.pack_forget()
    attack_name.pack_forget()
    packet_num.pack_forget()
    progress.pack_forget()
    packet_status.pack_forget()
    # btn_pogr.pack_forget()
    start_btn.pack_forget()
    stop_btn.pack_forget()
    

    # 입력창 초기화 및 다시 보여주기
    target_input.delete(0, tk.END)
    target_input.pack(padx=20, pady=20)
    
    attack_input.delete(0, tk.END)
    attack_input.pack(padx=20, pady=20)
    amount_input.delete(0, tk.END)
    amount_input.pack(padx=20, pady=20)
    btn_ip_show.config(command=ip_input)
    btn_ip_show.pack()
   
    root.update_idletasks()
    
# --- 초기 UI 구성 ---
target_input.pack(padx=20, pady=20)
attack_input.pack(padx=20, pady=20)
amount_input.pack(padx=20, pady=20)
btn_ip_show.config(command=ip_input)
btn_ip_show.pack()

# --- 재시작 버튼 (항상 오른쪽 하단) ---
btn_refresh.config(command=refresh)
btn_refresh.place(relx=1.0, rely=1.0, anchor="se") 
root.mainloop()
