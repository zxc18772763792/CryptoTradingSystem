# crypto_trading conda 鐜涓庢湇鍔¤繍琛屾€佹帓鏌ヨ鏄?
> 鐢熸垚鏃堕棿锛?026-05-23 20:53:46 +08:00
> 浠撳簱锛歚E:\9_Crypto\crypto_trading_system`
> 褰撳墠鍒嗘敮锛歚codex/operating-reality-bug-sweep`
> 褰撳墠鎻愪氦锛歚40922fb`
> 缁撹绫诲瀷锛氬彧璇昏瘖鏂鏄庛€傛湰鏂囨湭鎵ц浜ゆ槗妯″紡鍒囨崲銆?
---

## 1. 缁撹鎽樿

杩欐闂涓嶆槸鈥滄満鍣ㄤ笂娌℃湁 `crypto_trading` conda 鐜鈥濄€傜幆澧冪‘瀹炲瓨鍦紝`web.bat` 榛樿涔熺‘瀹炰娇鐢ㄥ畠鍚姩鏈嶅姟銆?
瀹為檯闂鏈変笁绫伙細

1. `crypto_trading` 鐜瀛樺湪锛屼絾瀹冩槸 `Python 3.9.25`锛岃€?README 鍜?Dockerfile 閮芥寚鍚?`Python 3.11`銆?2. `crypto_trading` 鐜鐨勪緷璧栧畨瑁呴泦涓?`requirements.txt` 涓嶄竴鑷达細鏈嶅姟鍚姩鍓嶇己 `defusedxml`锛涘綋鍓嶄粛缂?`pytest-asyncio` 鍜?`pytest-timeout`銆?3. 鏈嶅姟閫氳繃 guarded startup 鍚姩鏃剁‘瀹炶繘鍏ヤ簡 `paper`锛屼絾杩愯绾?19 鍒嗛挓鍚庡張琚繍琛屾椂璺緞鍒囧埌浜?`live`銆傝繖涓嶆槸 conda 鐜閫夋嫨闂锛岃€屾槸杩愯鏈熸ā寮忓垏鎹?鎸佷箙鍖栫姸鎬侀棶棰橈紝闇€瑕佸崟鐙拷鏌ャ€?
---

## 2. 鐜瀛樺湪鎬т笌瑙ｉ噴鍣ㄨВ鏋?
### 2.1 conda 鐜鍒楄〃

鍛戒护锛?
```powershell
where.exe conda
conda env list
```

鍏抽敭杈撳嚭锛?
```text
C:\ProgramData\anaconda3\Library\bin\conda.bat
C:\ProgramData\anaconda3\Scripts\conda.exe
C:\ProgramData\anaconda3\condabin\conda.bat

base                  *  C:\ProgramData\anaconda3
crypto_trading           C:\Users\lenovo\.conda\envs\crypto_trading
```

缁撹锛歚crypto_trading` 鐜瀛樺湪锛岃矾寰勬槸锛?
```text
C:\Users\lenovo\.conda\envs\crypto_trading
```

### 2.2 web.bat 榛樿鐜

`web.bat` 璋冪敤锛?
```text
scripts\web.ps1
```

`scripts\web.ps1` 鍙傛暟榛樿鍊硷細

```powershell
[string]$EnvName = "crypto_trading"
```

`scripts\web.ps1 start` 鍐嶈皟鐢細

```text
scripts\start_web_ps.ps1
```

骞跺皢 `-EnvName $EnvName` 浼犻€掕繘鍘汇€?
鍚姩 transcript 涔熺‘璁や簡瀹為檯瑙ｉ噴鍣細

```text
Using conda env: crypto_trading
Python executable: C:\Users\lenovo\.conda\envs\crypto_trading\python.exe
```

缁撹锛歚web.bat start` 娌℃湁璺?base Python锛涘畠璺戠殑鏄?`crypto_trading` 鐜閲岀殑 Python銆?
---

## 3. 榛樿 shell Python 涓庢湇鍔?Python 涓嶅悓

### 3.1 榛樿 shell Python

鍛戒护锛?
```powershell
python -c "import sys; print(sys.executable); print(sys.version)"
```

鍏抽敭杈撳嚭锛?
```text
C:\ProgramData\anaconda3\python.exe
3.11.5
```

### 3.2 鏈嶅姟 Python

鍛戒护锛?
```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -c "import sys; print(sys.executable); print(sys.version)"
```

鍏抽敭杈撳嚭锛?
```text
C:\Users\lenovo\.conda\envs\crypto_trading\python.exe
3.9.25
```

### 3.3 褰卞搷

鍓嶉潰鍏ㄩ噺娴嬭瘯浣跨敤鐨勬槸榛樿 shell Python锛?
```text
C:\ProgramData\anaconda3\python.exe
Python 3.11.5
pytest 7.4.0
```

鏈嶅姟浣跨敤鐨勬槸锛?
```text
C:\Users\lenovo\.conda\envs\crypto_trading\python.exe
Python 3.9.25
pytest 8.4.2
```

杩欎細瀵艰嚧鍚屼竴浠戒唬鐮佸湪娴嬭瘯鍜屾湇鍔＄幆澧冮噷琛ㄧ幇涓嶅悓銆?
涓€涓凡缁忓鐜扮殑渚嬪瓙鏄柊澧炵殑 `tests/test_order_manager_safety.py`锛氬畠鍦ㄩ粯璁?Python 3.11 涓嬮€氳繃锛屼絾鍦?`crypto_trading` Python 3.9 涓嬪け璐ワ細

```text
RuntimeError: There is no current event loop in thread 'MainThread'.
```

澶辫触鐐癸細

```text
core\trading\order_manager.py:76
self._client_order_lock = asyncio.Lock()

core\marketdata\ws_client.py:58
self._stop_event = asyncio.Event()
```

杩欑被宸紓绗﹀悎 Python 3.9 涓?Python 3.11 鍦?asyncio primitive 鍒濆鍖栬涓轰笂鐨勫樊鍒€傞」鐩枃妗ｈ姹?Python 3.11锛屽洜姝ゅ綋鍓?`crypto_trading` 鐜鐗堟湰鍋忕浜嗛」鐩０鏄庛€?
---

## 4. 渚濊禆鐘舵€?
### 4.1 褰撳墠 `crypto_trading` 鐜鍐呭叧閿寘

鍛戒护锛?
```powershell
conda list -n crypto_trading | rg -n "^(python|pytest|pytest-asyncio|pytest-timeout|defusedxml|feedparser|uvicorn|fastapi|pandas|numpy)\s"
```

鍏抽敭杈撳嚭锛?
```text
defusedxml  0.7.1
fastapi     0.128.8
feedparser  6.0.12
numpy       1.26.4
pandas      2.0.3
pytest      8.4.2
python      3.9.25
uvicorn     0.24.0
```

缂哄け椤癸細

```text
pytest-asyncio
pytest-timeout
```

### 4.2 Python import 妫€鏌?
鍛戒护锛?
```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -c "import importlib.util as u; [print(m, bool(u.find_spec(m))) for m in ['pytest_asyncio','pytest_timeout','defusedxml','uvicorn','fastapi']]"
```

鍏抽敭杈撳嚭锛?
```text
pytest_asyncio False
pytest_timeout False
defusedxml True
uvicorn True
fastapi True
```

### 4.3 defusedxml 鍚姩澶辫触鍘熷洜

绗竴娆￠噸鍚け璐ユ椂锛屾棩蹇椾腑鏄庣‘鎶ラ敊锛?
```text
ModuleNotFoundError: No module named 'defusedxml'
```

瑙﹀彂閾捐矾锛?
```text
web.main
core.ops.service.api
core.news.service.api
core.news.collectors.manager
core.news.collectors.rss
from defusedxml import ElementTree as ET
```

杩欒鏄庡綋鏃?`crypto_trading` 鐜瀛樺湪锛屼絾渚濊禆娌℃湁鍚屾鍒板綋鍓嶄唬鐮併€傚悗缁凡缁忔墽琛岋細

```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -m pip install 'defusedxml>=0.7,<1'
```

骞舵彁浜わ細

```text
40922fb Add RSS XML parser dependency
```

鐜板湪 `defusedxml` 宸插畨瑁呬笖宸插啓鍏?`requirements.txt`銆?
---

## 5. pytest warning 鐨勭湡瀹炲師鍥?
`pytest.ini` 鍖呭惈锛?
```ini
asyncio_mode = auto
timeout = 60
```

浣嗗綋鍓嶄袱涓?Python 鐜閮界己鑷冲皯涓€涓浉鍏虫彃浠讹細

榛樿 Python 3.11锛?
```text
pytest_asyncio False
pytest_timeout False
```

`crypto_trading` Python 3.9锛?
```text
pytest_asyncio False
pytest_timeout False
```

鍥犳 pytest 浼氳緭鍑猴細

```text
PytestConfigWarning: Unknown config option: asyncio_mode
PytestConfigWarning: Unknown config option: timeout
```

`requirements.txt` 褰撳墠宸茬粡澹版槑锛?
```text
pytest>=8.3,<9
pytest-asyncio>=0.25,<1
pytest-cov>=6,<7
pytest-timeout>=2.3,<3
```

鎵€浠?warning 鐨勬牴鍥犱笉鏄厤缃敊锛岃€屾槸褰撳墠瑙ｉ噴鍣ㄧ幆澧冩病鏈夋寜 `requirements.txt` 瀹屾暣鍚屾銆?
---

## 6. Python 鐗堟湰鍋忕

README 瑕佹眰锛?
```powershell
conda create -n crypto_trading python=3.11 -y
conda activate crypto_trading
pip install -r requirements.txt
```

Dockerfile 涔熶娇鐢細

```dockerfile
FROM python:3.11-slim
```

浣嗘湰鏈?`crypto_trading` 鏄細

```text
Python 3.9.25
```

杩欎笉鏄竴涓皬宸紓銆傚凡缁忚瀵熷埌鐨勫奖鍝嶏細

1. Python 3.9 涓嬪悓姝ユ祴璇曚腑鐩存帴鏋勯€?`asyncio.Lock()` / `asyncio.Event()` 浼氬け璐ャ€?2. 鏈嶅姟鍚姩鏃跺洜涓?uvicorn 宸茬粡鍒涘缓浜嬩欢寰幆锛屾墍浠ュ悓鏍蜂唬鐮佽矾寰勬湭蹇呭け璐ワ紝瀵艰嚧鈥滄湇鍔¤兘璺戯紝浣嗘祴璇曞け璐モ€濈殑涓嶄竴鑷淬€?3. 鏈潵缁х画鍔犲叆 asyncio 鐩稿叧浠ｇ爜鏃讹紝榛樿 shell 娴嬭瘯閫氳繃骞朵笉鑳借瘉鏄?`crypto_trading` 鏈嶅姟鐜涔熼€氳繃銆?
---

## 7. 鏈嶅姟褰撳墠杩愯鎬侊細鍚姩 paper锛屼箣鍚庡垏鍥?live

### 7.1 褰撳墠鐘舵€?
鍛戒护锛?
```powershell
.\web.bat status
Invoke-WebRequest http://127.0.0.1:8000/api/status
```

褰撳墠鍏抽敭杈撳嚭锛?
```text
Web          : running (PID=54288, state=healthy, mode=live)
Startup mode : configured=paper, persisted=live, source=guarded_configured
Guard        : blocked persisted live-mode restore during startup.
Warning      : service is currently running in live mode.
```

API 鐘舵€侊細

```json
{
  "trading_mode": "live",
  "paper_trading": false,
  "runtime": {
    "account_scope": "live",
    "startup_mode": {
      "configured_mode": "paper",
      "persisted_mode": "live",
      "source": "guarded_configured",
      "blocked_persisted_live_restore": true
    }
  }
}
```

### 7.2 鍚姩鏃剁‘瀹炴槸 paper

鏈鍚姩鏃ュ織 `logs\uvicorn_web_20260523_203045.err.log`锛?
```text
20:31:13 Blocked persisted live-mode restore during startup.
20:31:13 Paper trading mode: True
20:31:13 Risk manager baseline reset for scope=paper
20:31:13 Execution engine default trading mode: paper
20:31:13 Synchronized main account mode to paper after blocking persisted live-mode restore.
20:31:13 Startup trading mode resolved: effective=paper, configured=paper, persisted=live, source=guarded_configured
20:31:13 Execution engine started (default trading mode: paper)
```

杩欒鏄?guarded startup 閫昏緫鐢熸晥浜嗐€?
### 7.3 杩愯涓張琚垏鍒?live

鍚屼竴鏃ュ織绋嶅悗鍑虹幇锛?
```text
20:50:39 Paper trading mode: False
20:50:40 Risk manager scope switched: paper -> live
20:50:40 Risk manager baseline reset for scope=live
20:50:40 Execution engine default trading mode: live
```

杩欒鏄庢湇鍔′笉鏄€滃惎鍔ㄦ椂鐩存帴杩涘叆 live鈥濓紝鑰屾槸鍚姩鍚庤繍琛岀害 19 鍒嗛挓琚煇涓繍琛屾湡璺緞鍒囨崲鍒?live銆?
### 7.4 accounts.json 宸插啀娆℃樉绀?main=live

鍙鎽樿妫€鏌ワ細

```text
account_count 67
main mode live
mode_counts {'live': 15, 'paper': 52}
```

杩欎笌褰撳墠 `/api/status` 鐨?`live` 涓€鑷淬€?
### 7.5 褰撳墠涓嶈兘浠庢棩蹇楃‘瀹氳Е鍙戞簮

鍙鏃ュ織鍙褰曚簡搴曞眰缁勪欢鍒囨崲锛?
```text
order_manager.set_paper_trading(False)
risk_manager scope paper -> live
execution_engine default trading mode live
```

浣嗘病鏈夎褰曟槸鍝竴涓?HTTP 璺敱銆乁I 鎿嶄綔銆佸悗鍙颁换鍔℃垨 token 纭瑙﹀彂浜嗚鍒囨崲銆傜幇鏈夎闂棩蹇楁病鏈夎冻澶熺殑 route-level 淇℃伅鏉ョ洿鎺ュ綊鍥犮€?
鍙枒璺緞搴旂户缁粠杩欎簺浣嶇疆鏌ワ細

```text
web\api\trading_runtime.py
web\services\trading_runtime_service.py
core\trading\execution_engine.py
data\config\accounts.json
data\ai_runtime_config.json
```

---

## 8. 褰撳墠椋庨櫓鍒ゆ柇

### 8.1 鐜椋庨櫓

`crypto_trading` 鐜瀛樺湪锛屼絾涓嶆槸椤圭洰澹版槑鐜銆傚綋鍓嶇姸鎬佹槸锛?
```text
椤圭洰澹版槑锛歅ython 3.11
鏈嶅姟鐜锛歅ython 3.9.25
榛樿娴嬭瘯鐜锛歅ython 3.11.5 base
```

杩欎細璁┾€滄祴璇曢€氳繃鈥濆拰鈥滄湇鍔¤繍琛屸€濆け鍘讳竴鑷存€с€?
### 8.2 渚濊禆椋庨櫓

褰撳墠鏈嶅姟鑳藉惎鍔紝鏄洜涓哄凡缁忎复鏃跺畨瑁呬簡 `defusedxml` 骞舵彁浜ゅ埌 `requirements.txt`銆備絾鐜浠嶆湭瀹屾暣鍚屾锛?
```text
pytest-asyncio missing
pytest-timeout missing
```

杩欎細缁х画閫犳垚 pytest 閰嶇疆 warning锛屼篃浼氬奖鍝?async 娴嬭瘯琛屼负銆?
### 8.3 浜ゆ槗妯″紡椋庨櫓

褰撳墠鏈嶅姟瀹為檯鏄?`live`锛?
```text
trading_mode=live
paper_trading=false
```

杩欎笌榛樿 managed startup 鐨勫畨鍏ㄧ洰鏍?`paper` 涓嶄竴鑷淬€傚洜涓烘湰娆＄敤鎴疯姹傛槸鈥滄帓鏌ュ苟鍐欒鏄庢枃妗ｂ€濓紝鏈枃娌℃湁鎵ц鍒囧洖 paper 鎴栭噸鍚搷浣溿€?
---

## 9. 寤鸿澶勭悊椤哄簭

### P0锛氬厛澶勭悊褰撳墠 live 婕傜Щ

濡傛灉鐩爣鏄户缁?paper long-run锛屽簲鎵ц鍙楁帶鍒囨崲鎴栭噸鍚細

```powershell
.\web.bat stop -IncludeWorkers
.\web.bat start
.\web.bat status
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/status
```

浣嗙敱浜庡綋鍓嶆湇鍔″凡缁忔槸 live锛屽缓璁湪鎵ц鍓嶅厛纭鏄惁鏈夋鍦ㄨ繍琛岀殑 live 绛栫暐銆佺湡瀹炰粨浣嶃€佹湭瀹屾垚璁㈠崟銆?
闇€瑕佽ˉ鐨勪唬鐮?瀹¤鑳藉姏锛?
1. 瀵?`switch_trading_mode_service(...)` 澧炲姞鏄庣‘瀹¤鏃ュ織锛氳皟鐢ㄨ矾鐢便€佽姹傜敤鎴?鏉ユ簮銆佺洰鏍囨ā寮忋€乼oken銆乧onfirm text 鏄惁閫氳繃銆?2. 瀵?guarded startup 鍚庣殑杩愯鏈?live 鍒囨崲鍔犱簩娆￠槻绾匡細濡傛灉鍚姩鏃?`blocked_persisted_live_restore=true`锛岄櫎闈炴樉寮?live approval锛屽惁鍒欎笉鍏佽鍚庡彴鎴栨櫘閫?UI 娴佺▼鎶婇粯璁ゆ墽琛屾ā寮忓垏鍥?live銆?3. 瀵?`accounts.json` 涓?`main` 璐︽埛琚啀娆″啓鎴?live 鐨勮矾寰勫姞娴嬭瘯銆?
### P1锛氬榻?conda 鐜鍒?Python 3.11

淇濆畧鏂规鏄厛鍒涘缓鏂扮幆澧冮獙璇侊紝涓嶇洿鎺ヨ鐩栫幇鏈夌幆澧冿細

```powershell
conda create -n crypto_trading_311 python=3.11 -y
conda activate crypto_trading_311
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest tests/test_web_main_runtime_tasks.py tests/test_order_manager_safety.py -q
.\web.bat start -EnvName crypto_trading_311
```

楠岃瘉绋冲畾鍚庯紝鍐嶅喅瀹氭槸鍚︽妸榛樿 `crypto_trading` 鐜閲嶅缓涓?Python 3.11銆?
### P2锛氬悓姝ュ綋鍓?`crypto_trading` 鐨勬祴璇曟彃浠?
濡傛灉鐭湡杩樼户缁娇鐢ㄥ綋鍓?Python 3.9 鐜锛岃嚦灏戝簲琛ワ細

```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -m pip install "pytest-asyncio>=0.25,<1" "pytest-timeout>=2.3,<3"
```

浣嗚繖鍙兘娑堥櫎 pytest config warning锛屼笉鑳借В鍐?Python 3.9 涓庨」鐩?Python 3.11 鐨勮涓哄樊寮傘€?
### P3锛氱粺涓€鏃ュ父楠岃瘉鍏ュ彛

浠ュ悗鎻愪氦鍓嶅缓璁槑纭娇鐢ㄦ湇鍔″悓娆?Python锛?
```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -m pytest -q
```

鎴栬€呭湪 Python 3.11 鐜瀹屾垚鍚庝娇鐢細

```powershell
& 'C:\Users\lenovo\.conda\envs\crypto_trading_311\python.exe' -m pytest -q
```

涓嶈鍐嶇敤瑁?`python -m pytest` 浣滀负鍞竴缁撹锛岄櫎闈炲厛纭锛?
```powershell
python -c "import sys; print(sys.executable); print(sys.version)"
```

---

## 10. 鏈鎺掓煡鍛戒护娓呭崟

```powershell
git status --short --branch
where.exe conda
conda env list
python -c "import sys; print(sys.executable); print(sys.version)"
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -c "import sys; print(sys.executable); print(sys.version)"
conda list -n crypto_trading
& 'C:\Users\lenovo\.conda\envs\crypto_trading\python.exe' -m pip list
.\web.bat status
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/status
Get-Content logs\uvicorn_web_20260523_203045.err.log -Tail 260
```

---

## 11. 鏈€缁堝垽鏂?
`crypto_trading` conda 鐜瀛樺湪锛屾湇鍔′篃纭疄鐢ㄥ畠鍚姩銆傚墠闈㈠惎鍔ㄥけ璐ョ殑鐩存帴鍘熷洜鏄鐜缂?`defusedxml`锛屼笉鏄幆澧冧笉瀛樺湪銆?
浣嗘洿澶х殑闂鏄幆澧冩紓绉伙細

```text
crypto_trading: Python 3.9.25
椤圭洰澹版槑: Python 3.11
requirements: 閮ㄥ垎渚濊禆鏈悓姝ュ埌鐜
```

浠ュ強杩愯鎬佹紓绉伙細

```text
20:31 guarded startup -> paper
20:50 runtime switch -> live
```

寤鸿涓嬩竴姝ヤ紭鍏堝鐞?live 婕傜Щ鐨勮Е鍙戞簮鍜屽璁＄己鍙ｏ紝鐒跺悗鎶?`crypto_trading` 瀵归綈鍒?Python 3.11锛屽苟鐢ㄥ悓涓€涓В閲婂櫒璺戞祴璇曞拰鏈嶅姟銆?
