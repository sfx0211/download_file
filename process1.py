import click
from concurrent import futures
from functools import partial
import json
import os
import threading
import time
from tqdm import tqdm
from request_ import request_


def _fetchByRange(lock, url, temp_filename, config_filename, part_number, start, stop):
    '''根据 HTTP headers 中的 Range 只下载一个块 (rb+ 模式)
    lock: 互斥锁
    url: 远程目标文件的 URL 地址
    temp_filename: 临时文件
    config_filename: 配置文件
    part_number: 块编号(从 0 开始)
    start: 块的起始位置
    stop: 块的结束位置
    '''
    headers = {'Range': 'bytes=%d-%d' % (start, stop)}
    r = request_('GET', url, headers=headers)

    part_length = stop - start + 1
    if not r or len(r.content) != part_length:  # 请求失败时，r 为 None
        return {
            'failed': True  # 用于告知 _fetchByRange() 的调用方，此 Range 下载失败了
        }

    # 此分块的信息
    part = {
        'ETag': r.headers['ETag'],
        'Last-Modified': r.headers['Last-Modified'],
        'PartNumber': part_number,
        'Size': part_length
    }

    # 获取锁
    lock.acquire()
    try:
        with open(temp_filename, 'rb+') as fp:
            fp.seek(start)  # 移动文件指针
            fp.write(r.content)  # 写入已下载的字节
        # 读取原配置文件中的内容
        f = open(config_filename, 'r')
        cfg = json.load(f)
        f.close()
        # 更新配置文件，写入此分块的信息
        f = open(config_filename, 'w')
        cfg['parts'].append(part)
        json.dump(cfg, f)
        f.close()
    except Exception as e:
        return {
            'failed': True  #  Range 下载失败了
        }
    finally:
        # 释放锁
        lock.release()
    return {
        'part': part,
        'failed': False  #  Range 成功下载
    }


@click.command()
@click.option('--dest_filename', type=click.Path(), help="Name of the local destination file with extension")
@click.option('--multipart_chunksize', default=8*1024*1024, help="Size of chunk, unit is bytes")
@click.argument('url', type=click.Path())
def download_file(dest_filename, multipart_chunksize, url):
    t0 = time.time()

    # 如果没有指定本地保存时的文件名，则默认使用 URL 中最后一部分作为文件名
    official_filename = dest_filename if dest_filename else url.split('/')[-1]  # 正式文件名
    temp_filename = official_filename + '.swp'  # 没下载完成时，临时文件名
    config_filename = official_filename + '.swp.cfg'  # 没下载完成时，存储 ETag 等信息的配置文件名

    # 获取文件的大小和 ETag
    r = request_('HEAD', url)
    if not r:  # 请求失败时，r 为 None
        return
    file_size = int(r.headers['Content-Length'])
    ETag = r.headers['ETag']

    # 如果正式文件存在
    if os.path.exists(official_filename):
        if os.path.getsize(official_filename) == file_size:  # 且大小与待下载的目标文件大小一致时
            return
        else:  # 大小不一致时，提醒用户要保存的文件名已存在，需要手动处理，不能随便覆盖
            return

    # 首先需要判断此文件支不支持 Range 下载，请求第 1 个字节即可
    headers = {'Range': 'bytes=0-0'}
    r = request_('HEAD', url, headers=headers)
    if not r:  # 请求失败时，r 为 None
        return

    if r.status_code != 206:  # 不支持 Range 下载时
        # 需要重新从头开始下载 (wb 模式)
        with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, desc=official_filename) as bar:  # 打印下载时的进度条，并动态显示下载速度
            r = request_('GET', url, stream=True)
            if not r:  # 请求失败时，r 为 None
                return
            with open(temp_filename, 'wb') as fp:
                for chunk in r.iter_content(chunk_size=multipart_chunksize):
                    if chunk:
                        fp.write(chunk)
                        bar.update(len(chunk))
        # 整个文件内容被成功下载后，将临时文件名修改回正式文件名、删除配置文件
        os.rename(temp_filename, official_filename)
        if os.path.exists(config_filename):
            os.remove(config_filename)
    else:  # 支持 Range 下载时
        # 获取文件的总块数
        div, mod = divmod(file_size, multipart_chunksize)
        parts_count = div if mod == 0 else div + 1  # 计算出多少个分块

        # 如果临时文件存在
        if os.path.exists(temp_filename):
            if os.path.getsize(temp_filename) != file_size:  # 说明此临时文件有问题，需要先删除它
                os.remove(temp_filename)
            else:  # 临时文件有效时
                if not os.path.exists(config_filename):  # 如果不存在配置文件时
                    os.remove(temp_filename)
                else:  # 如果配置文件也在，则继续判断 ETag 是否一致
                    with open(config_filename, 'r') as fp:
                        cfg = json.load(fp)
                        if cfg['ETag'] != ETag:  # 如果不一致
                            os.remove(temp_filename)
                        else:  # 从配置文件中读取已下载的分块号集合，从而得出未下载的分块号集合
                            succeed_parts = {part['PartNumber'] for part in cfg['parts']}  # 之前已下载好的分块号集合
                            succeed_parts_size = sum([part['Size'] for part in cfg['parts']])  # 已下载的块的总大小，注意是列表推导式不是集合推导式
                            parts = set(range(parts_count)) - succeed_parts  # 本次需要下载的分块号集合

        # 再次判断临时文件在不在，如果不存在时，表示要下载所有分块号
        if not os.path.exists(temp_filename):
            succeed_parts_size = 0
            parts = range(parts_count)

            # 由于 _fetchByRange() 中使用 rb+ 模式，必须先保证文件存在，所以要先创建指定大小的临时文件 (用0填充)
            f = open(temp_filename, 'wb')
            f.seek(file_size - 1)
            f.write(b'\0')
            f.close()

            with open(config_filename, 'w') as fp:  # 创建配置文件，写入 ETag
                cfg = {
                    'ETag': ETag,
                    'parts': []
                }
                json.dump(cfg, fp)

        # 多线程并发下载
        workers = min(8, len(parts))
        failed_parts = 0  # 下载失败的分块数目

        # 创建互斥锁
        lock = threading.Lock()

        # 固定住 lock、url、temp_filename、config_filename，不用每次都传入相同的参数
        _fetchByRange_partial = partial(_fetchByRange, lock, url, temp_filename, config_filename)

        with futures.ThreadPoolExecutor(workers) as executor:
            to_do = []
            # 创建并排定Future
            for part_number in parts:
                # 通过块号计算出块的起始与结束位置，最后一块(编号从0开始，所以最后一块编号为 parts_count - 1)需要特殊处理
                if part_number != parts_count-1:
                    start = part_number * multipart_chunksize
                    stop = (part_number + 1) * multipart_chunksize - 1
                else:
                    start = part_number * multipart_chunksize
                    stop = file_size - 1
                future = executor.submit(_fetchByRange_partial, part_number, start, stop)
                to_do.append(future)

            # 获取Future的结果，futures.as_completed(to_do)的参数是Future列表，返回迭代器，
            # 只有当有Future运行结束后，才产出future
            done_iter = futures.as_completed(to_do)
            with tqdm(total=file_size, initial=succeed_parts_size, unit='B', unit_scale=True, unit_divisor=1024, desc=official_filename) as bar:  # 打印下载时的进度条，并动态显示下载速度
                for future in done_iter:  # future变量表示已完成的Future对象，所以后续future.result()绝不会阻塞
                    result = future.result()
                    if result.get('failed'):
                        failed_parts += 1
                    else:
                        bar.update(result.get('part')['Size'])

        if failed_parts <= 0:
            # 整个文件内容被成功下载后，将临时文件名修改回正式文件名、删除配置文件
            os.rename(temp_filename, official_filename)
            if os.path.exists(config_filename):
                os.remove(config_filename)


if __name__ == '__main__':
    download_file()
