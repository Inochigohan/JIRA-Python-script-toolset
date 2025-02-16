import time
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import sys
from jira import JIRA


def safe_get(data, keys, default="字段内容为空"):
    """安全获取嵌套字典字段"""
    for key in keys.split('.'):
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
        if data == default:
            break
    return data if data not in [None, ""] else default


def convert_time(jira_time):
    """转换JIRA时间格式"""
    try:
        dt = datetime.strptime(jira_time, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return jira_time


def get_single_choice(prompt, options):
    """获取严格单项选择"""
    options_str = "/".join(options)
    while True:
        choice = input(prompt).strip().lower()
        if len(choice) == 1 and choice in options:
            return choice
        print(f"无效输入! 请选择[{options_str}]中的一个选项")


def get_multi_choice(prompt, options):
    """获取严格多项选择"""
    options_str = ",".join(options)
    while True:
        choice = input(prompt).strip().lower().replace('，', ',')
        if not choice:
            continue
        selections = [c.strip() for c in choice.split(',')]
        if all(c in options for c in selections):
            return list(set(selections))  # 去重
        print(f"无效输入! 有效选项为[{options_str}]，多个选项用英文逗号分隔")


# 修改后的交互函数
def get_auth_method():
    """获取认证方式"""
    prompt = "\n1、请选择登录方式：\na.账号密码\nb.API令牌\n请输入选项(a/b): "
    return get_single_choice(prompt, ['a', 'b'])


def get_fetch_method():
    """获取数据获取方式"""
    prompt = "\n2、请选择数据获取方式：\na.Python Jira库\nb.REST API\n请输入选项(a/b): "
    return get_single_choice(prompt, ['a', 'b'])


def get_jql():
    """获取JQL查询语句"""
    while True:
        choice = input("\n3、请输入JQL查询语句（建议添加时间范围如'created >= -7d'）: ").strip()
        if not choice:
            print("输入不能为空，请重新输入！")
        else:
            break
    return choice

def get_output_format():
    """获取输出格式"""
    prompt = "\n4、请选择输出格式（可多选）：\na.CSV文件\nb.Excel文件\nc.TXT文件\n请输入选项(如a,b): "
    return get_multi_choice(prompt, ['a', 'b', 'c'])


def get_output_base():
    """获取输出文件前缀"""
    while True:
        choice = input("\n5、请输入输出文件前缀: ").strip()
        if not choice:
            print("输入不能为空，请重新输入！")
        else:
            break
    return choice


def get_jira_server():
    """获取JIRA服务器地址"""
    while True:
        choice = input("\n6、请输入JIRA服务器地址: ").strip()
        if not choice:
            print("输入不能为空，请重新输入！")
        else:
            break
    return choice

def get_username():
    """获取JIRA帐号用户名"""
    while True:
        choice = input("\n7、请输入JIRA帐号用户名: ").strip()
        if not choice:
            print("输入不能为空，请重新输入！")
        else:
            break
    return choice

def get_password():
    """获取JIRA帐号密码/API令牌"""
    while True:
        choice = input("\n8、请输入密码/API令牌: ").strip()
        if not choice:
            print("输入不能为空，请重新输入！")
        else:
            break
    return choice

# 数据获取与处理函数保持不变
def fetch_via_library(jira_server, auth_method, username, password, jql):
    """使用Jira库获取数据"""
    try:
        auth = (username, password)
        jira = JIRA(
            server=jira_server,
            basic_auth=auth,
            timeout=30
        )

        all_issues = []
        block_size = 100
        start_at = 0

        with tqdm(desc="获取数据进度") as pbar:
            while True:
                issues = jira.search_issues(
                    jql_str=jql,
                    startAt=start_at,
                    maxResults=block_size,
                    expand='comments'
                )
                all_issues.extend(issues)
                start_at += len(issues)
                pbar.update(len(issues))
                if len(issues) < block_size:
                    break
        return [issue.raw for issue in all_issues]
    except Exception as e:
        print(f"Jira库获取数据失败: {str(e)}")
        sys.exit(1)


def fetch_via_api(jira_server, username, password, jql):
    """使用REST API获取数据"""
    headers = {'Accept': 'application/json'}
    auth = HTTPBasicAuth(username, password)
    search_url = f'{jira_server}/rest/api/2/search'
    all_issues = []
    start_at = 0
    max_results = 100

    try:
        with tqdm(desc="获取数据进度") as pbar:
            while True:
                params = {
                    'jql': jql,
                    'startAt': start_at,
                    'maxResults': max_results,
                    'expand': 'comments',
                    'fields': 'key,issuetype,summary,created,status,resolution,environment,description,comment'
                }

                response = requests.get(
                    search_url,
                    headers=headers,
                    auth=auth,
                    params=params,
                    timeout=60
                )

                if response.status_code != 200:
                    print(f"请求失败: {response.status_code}\n{response.text}")
                    sys.exit(1)

                data = response.json()
                issues = data.get('issues', [])
                all_issues.extend(issues)
                start_at += len(issues)
                pbar.update(len(issues))

                if start_at >= data.get('total', 0):
                    break
        return all_issues
    except Exception as e:
        print(f"API请求失败: {str(e)}")
        sys.exit(1)


def process_issues(issues):
    """统一处理issues数据"""
    processed = []
    for issue in tqdm(issues, desc="处理数据"):
        try:
            fields = issue.get('fields', {})
            base = {
                # ... 其他字段保持不变 ...
                'key': safe_get(issue, 'key'),
                'type': safe_get(fields, 'issuetype.name'),
                'Summary': safe_get(fields, 'summary'),
                'created': convert_time(safe_get(fields, 'created')),
                'status': safe_get(fields, 'status.name'),
                'resolution': safe_get(fields, 'resolution.name'),
                'environment': safe_get(fields, 'environment'),
                'description': safe_get(fields, 'description'),
                'comments': safe_get(fields, 'comment.comments', default=[])
            }
            base['created_year'], base['created_month'] = (
                base['created'][:4],
                base['created'][5:7]
            ) if base['created'] != '字段内容为空' else ('字段内容为空', '字段内容为空')

            # 处理评论数据
            comment_list = []
            if base['comments'] and isinstance(base['comments'], list):
                for comment in base['comments']:
                    comment_data = {
                        'body': safe_get(comment, 'body'),
                        # 直接提取displayName字段
                        'author': safe_get(comment, 'author.displayName'),
                        'created': convert_time(safe_get(comment, 'created'))
                    }
                    comment_list.append(comment_data)

            processed.append({
                **base,
                'comment_data': comment_list  # 存储处理后的评论列表
            })

        except Exception as e:
            print(f"处理失败: {str(e)}")
            continue
    return processed


def generate_txt(data, filename):
    """生成TXT文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data:
            # ... 其他字段写入保持不变 ...
            f.write("--------\n")
            f.write(f"key：{item['key']}\n")
            f.write(f"type：{item['type']}\n")
            f.write(f"Summary：{item['Summary']}\n")
            f.write(f"created：{item['created']}\n")
            f.write(f"created_year：{item['created_year']}\n")
            f.write(f"created_month：{item['created_month']}\n")
            f.write(f"status：{item['status']}\n")
            f.write(f"resolution：{item['resolution']}\n")
            f.write(f"environment：{item['environment']}\n")
            f.write(f"description：{item['description']}\n")

            f.write("all comment：\n")
            if not item['comment_data']:
                f.write("该工单无备注\n")
            else:
                # 遍历处理后的评论数据
                for idx, comment in enumerate(item['comment_data'], 1):
                    f.write(f"comment-{idx}：{comment['author']}{{comment author}}在")
                    f.write(f"{comment['created']}{{comment created time}}备注【{comment['body']}】\n")


def generate_csv(data, filename):
    """生成CSV文件"""
    df = format_dataframe(data)
    df.to_csv(filename, index=False, encoding='utf_8_sig')


def generate_excel(data, filename):
    """生成Excel文件"""
    df = format_dataframe(data)
    df.to_excel(filename, index=False, engine='openpyxl')

def format_dataframe(data):
    """格式化数据为DataFrame"""
    formatted = []
    for item in data:
        # 基础字段
        base_row = {
            'key': item['key'],
            'type': item['type'],
            'Summary': item['Summary'],
            'created': item['created'],
            'created_year': item['created_year'],
            'created_month': item['created_month'],
            'status': item['status'],
            'resolution': item['resolution'],
            'environment': item['environment'],
            'description': item['description']
        }

        # 评论处理
        if not item['comment_data']:
            # 没有评论的情况
            row = base_row.copy()
            row.update({
                'comment': '该issue无comment',
                'comment author': '',
                'comment created time': ''
            })
            formatted.append(row)
        else:
            # 每个评论生成独立行
            for comment in item['comment_data']:
                row = base_row.copy()
                row.update({
                    'comment': comment['body'],
                    'comment author': comment['author'],
                    'comment created time': comment['created']
                })
                formatted.append(row)

    return pd.DataFrame(formatted)

def main():
    start_time = time.time()

    # 交互式输入
    print("\n" + "=" * 40)
    print("JIRA数据导出工具 v1.0".center(40))
    print("=" * 40)

    auth_method = get_auth_method()
    fetch_method = get_fetch_method()
    jql = get_jql()
    output_formats = get_output_format()
    output_base = get_output_base()

    # 凭证获取
    jira_server = get_jira_server()
    username = get_username()
    password = get_password()

    # 数据获取逻辑保持不变
    print("\n" + "=" * 40)
    # 获取数据
    if 'a' in fetch_method:
        issues = fetch_via_library(jira_server, auth_method[0], username, password, jql)
    else:
        issues = fetch_via_api(jira_server, username, password, jql)

    # 处理数据
    processed_data = process_issues(issues)

    # 文件生成逻辑
    format_mapping = {
        'a': ('csv', generate_csv),
        'b': ('xlsx', generate_excel),
        'c': ('txt', generate_txt)
    }

    print("\n" + "=" * 40)
    for fmt in output_formats:
        ext, generator = format_mapping[fmt]
        filename = f"{output_base}.{ext}"
        try:
            generator(processed_data, filename)
            print(f"✓ 已生成 {filename}")
        except Exception as e:
            print(f"✗ 生成{ext.upper()}文件失败: {str(e)}")

    # 统计信息
    print("\n" + "=" * 40)
    print(f"总耗时: {time.time() - start_time:.2f}秒")
    print(f"处理Issue数: {len(issues)}")
    print(f"生成数据行: {len(processed_data)}")


if __name__ == "__main__":
    main()


