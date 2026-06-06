import subprocess, os, shutil

d = os.path.expanduser('~/fnb_raw/youtube')
tmp = os.path.expanduser('~/fnb_raw/youtube_eng')
os.makedirs(tmp, exist_ok=True)

# 한글 파일명 → 영문 임시 파일명으로 복사
files = [f for f in os.listdir(d) if f.endswith('.csv')]
print('total: {}'.format(len(files)))

mapping = {}
for i, f in enumerate(files):
    eng_name = 'yt_{:03d}.csv'.format(i)
    src = os.path.join(d, f)
    dst = os.path.join(tmp, eng_name)
    shutil.copy2(src, dst)
    mapping[eng_name] = f

# 영문 파일명으로 HDFS 업로드
env = os.environ.copy()
env['JAVA_TOOL_OPTIONS'] = '-Dfile.encoding=UTF-8'

for eng_name, orig_name in mapping.items():
    path = os.path.join(tmp, eng_name)
    # HDFS에 원래 한글 이름으로 저장
    hdfs_path = '/user/maria_dev/fnb/youtube/' + eng_name
    ret = subprocess.run(
        ['hdfs', 'dfs', '-put', path, hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )
    if ret.returncode == 0:
        print('OK: {}'.format(orig_name))
    else:
        print('FAIL: {} | {}'.format(orig_name, ret.stderr.decode('utf-8', errors='ignore').strip()))

print('done')
