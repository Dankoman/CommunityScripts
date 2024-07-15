import requests, os, shutil
import re, sys, json
import datetime as dt
import pathlib
import time
from inspect import getmembers, isfunction
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
import traceback

CurRunDir = pathlib.Path(__file__).parent.resolve()
vENVDir = f"{CurRunDir}/../PythonToolsInstaller/venv/lib/python3.11/site-packages"

try:
    if os.path.isdir(vENVDir):
        print(f"VENV Dir {vENVDir} used", file=sys.stderr)
        sys.path.insert(0, vENVDir)
    else:
        print(f"VENV Dir {vENVDir} not used", file=sys.stderr)

except Exception as e:
    # Ignore
    print("Hey there...")

try:
    import stashapi.log as log
    from stashapi.tools import human_bytes, human_bits
    from stashapi.stash_types import PhashDistance
    from stashapi.stashapp import StashInterface

except ModuleNotFoundError:
    print(
        "You need to install the stashapi module. (pip install stashapp-tools)",
        file=sys.stderr,
    )
    sys.exit(1)

FAKTORCONV = 6.25
FRAGMENT = json.loads(sys.stdin.read())
MODE = FRAGMENT["args"].get("mode")
PLUGIN_DIR = FRAGMENT["server_connection"]["PluginDir"]
stash = StashInterface(FRAGMENT["server_connection"])

SLIM_SCENE_FRAGMENT = """
id
title
url
urls
details
date
tags { id }
studio{
   name
   stash_ids{
      endpoint
      stash_id
   }
}
files {
    size
    path
    width
    height
    bit_rate
    mod_time
    duration
    frame_rate
    video_codec
}
"""

def create_session_with_retries():
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "OPTIONS"])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def rate_limited_request(url, session, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = session.get(url, allow_redirects=True)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            log.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.random()
                log.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                log.error(f"Max retries reached. Giving up on URL: {url}")
                raise

def main():
    log.info(f"Plugin Dir {PLUGIN_DIR} ")
    cachepath = os.path.join(PLUGIN_DIR, "cache")

    try:
        os.makedirs(cachepath, exist_ok=True)
        print("Directory '%s' created successfully" % cachepath)
    except OSError as error:
        print("Directory '%s' can not be created" % cachepath)

    if MODE:
        if MODE == "download":
            get_download()
        if MODE == "disable":
            return True
    else:
        FRAGMENT_HOOK_TYPE = FRAGMENT["args"]["hookContext"]["type"]
        FRAGMENT_SCENE_ID = FRAGMENT["args"]["hookContext"]["id"]
        try:
            get_download()  # ToDo use single Scene
        except Exception as err:
            log.error(f"main function error: {err}")
            traceback.print_exc()

    log.exit("Plugin exited normally.")

def parse_timestamp(ts, format="%Y-%m-%dT%H:%M:%S%z"):
    ts = re.sub(r"\.\d+", "", ts)  # remove fractional seconds
    return dt.datetime.strptime(ts, format)

def get_download():
    cachepath = os.path.join(PLUGIN_DIR, "cache")
    log.info(f"Plugin Cachepath {cachepath} ")

    scene_count, scenes = stash.find_scenes(
        f={
            "url": {
                "modifier": "MATCHES_REGEX",
                "value": 
"howwomenorgasm\\.com|switch\\.com|getupclose\\.com|milfoverload\\.net|dareweshare\\.net|jerkbuddies\\.com|adulttime\\.studio|adulttime\\.com|oopsie\\.tube|adulttimepilots\\.com|kissmefuckme\\.net|youngerloverofmine\\.com",
            }
        },
        fragment=SLIM_SCENE_FRAGMENT,
        get_count=True,
    )

    log.info(f"Plugin found {scene_count} Scenes from Adulttime ")

    session = create_session_with_retries()

    for i, scene in enumerate(scenes):
        if not scene_has_funscript(scene):
            try:
                process_scene(scene, cachepath, session)
            except Exception as e:
                log.error(f"Error processing scene {scene['id']}: {str(e)}")
        else:
            log.info(f"Scene {scene['id']} already has a funscript file. Skipping.")

        log.progress(i / scene_count)

def scene_has_funscript(scene):
    for file in scene["files"]:
        filepath = os.path.dirname(os.path.abspath(file["path"]))
        filename = os.path.basename(file["path"])
        filenamewithoutext = filename.rsplit(".", maxsplit=1)[0]
        funscriptnewname = f"{filenamewithoutext}.funscript"
        funscriptnewlocaton = os.path.join(filepath, funscriptnewname)
        if os.path.exists(funscriptnewlocaton):
            return True
    return False

def process_scene(scene, cachepath, session):
    title = re.sub(r"\[PDT: .+?\]\s+", "", scene["title"])
    urls = scene["urls"]

    for u in urls:
        if re.search(r"\.adulttime\.com", u):
            aid = re.search(r"\/([0-9]+)", u)
            aid = aid.group(1)
            fpw = f"{cachepath}/{aid}.json"
            fppatw = f"{cachepath}/{aid}.pat"
            fpfunw = f"{cachepath}/{aid}.funscript"
            log.debug(f"Found Adulttime URL {u} width Provider ID {aid}")

            if not os.path.isfile(fpw):
                download_and_process_pattern(aid, fpw, fppatw, fpfunw, scene, session)
            else:
                process_existing_pattern(fpw, fppatw, fpfunw, scene, session)

def download_and_process_pattern(aid, fpw, fppatw, fpfunw, scene, session):
    dlurl = f"https://coll.lovense.com/coll-log/video-websites/get/pattern?videoId={aid}&pf=Adulttime"
    try:
        r = rate_limited_request(dlurl, session)
        log.debug(r.content)
        dlapires = r.json()
        with open(fpw, "w+") as f:
            json.dump(dlapires, f)

        if dlapires["code"] == 0:
            dlpaturl = dlapires["data"]["pattern"]
            rpat = rate_limited_request(dlpaturl, session)
            with open(fppatw, "w+") as f:
                f.write(rpat.content.decode("utf-8"))
            convert_lovense_to_funscript(scene, fppatw, fpfunw)
            map_file_with_funscript(scene, fpfunw)
        else:
            log.debug(f"No Interactive for this ID")
    except Exception as e:
        log.error(f"Error downloading pattern for scene {scene['id']}: {str(e)}")
        raise

def process_existing_pattern(fpw, fppatw, fpfunw, scene, session):
    with open(fpw, "r") as f:
        dlapires = json.load(f)

    try:
        if dlapires["code"] == 0:
            log.info(f"Try Interactive for this ID")

            if not os.path.isfile(fppatw):
                dlpaturl = dlapires["data"]["pattern"]
                rpat = rate_limited_request(dlpaturl, session)
                with open(fppatw, "w+") as f:
                    f.write(rpat.content.decode("utf-8"))

            if not os.path.isfile(fpfunw):
                convert_lovense_to_funscript(scene, fppatw, fpfunw)

            map_file_with_funscript(scene, fpfunw)

        else:
            log.debug(f"No Interactive for this ID")

    except Exception as e:
        log.error(f"Error processing existing pattern for scene {scene['id']}: {str(e)}")
        if "Too many requests" in str(e) or "security" in str(e):
            os.remove(fpw)
            log.error("Too many requests. Wait a moment...")
            time.sleep(60)
        raise

def map_file_with_funscript(sceneinfo, funscriptfile):
    scenefiles = sceneinfo["files"]
    for u in scenefiles:
        filepath = os.path.dirname(os.path.abspath(u["path"]))
        filename = os.path.basename(u["path"])
        filenamewithoutext = filename.rsplit(".", maxsplit=1)[0]
        funscriptnewname = f"{filenamewithoutext}.funscript"
        funscriptnewlocaton = os.path.join(filepath, funscriptnewname)

        if not os.path.exists(funscriptnewlocaton):
            shutil.copy2(funscriptfile, funscriptnewlocaton)
            log.info(f"Copy {funscriptfile} to {funscriptnewlocaton}")
        else:
            log.info(f"Funscript already exists at {funscriptnewlocaton}. Skipping.")

def convert_lovense_to_funscript(sceneinfo, patternfile, funscriptfile):
    title = re.sub(r"\[PDT: .+?\]\s+", "", sceneinfo["title"])
    duration = int(sceneinfo["files"][0]["duration"] + 0.5) * 1000

    with open(patternfile, "r") as losc:
        lovensactions = json.load(losc)

    data = {
        "version": "1.0",
        "range": 100,
        "inverted": False,
        "metadata": {
            "bookmarks": {},
            "chapters": {},
            "performers": {},
            "tags": {},
            "title": title,
            "creator": "Adulttime Interactive Downloader for Stash",
            "description": "",
            "duration": duration,
            "license": "Open",
            "script_url": "",
            "type": "basic",
            "video_url": "",
            "notes": "Convert from Lovense to Funscript"
        },
        "actions": []
    }

    marker_at = 0
    marker_pos = 0
    for la in lovensactions:
        if la["v"] == 0:
            marker_at = 0
        else:
            marker_at = la["v"] * FAKTORCONV

        if la["t"] == 0:
            log.debug(f"Skip Junk with Value '{la['t']}'")
        else:
            marker_pos = la["t"] * 1
            data["actions"].append({"pos": int(marker_at + 0.5), "at": int(marker_pos + 0.5)})

    with open(funscriptfile, "w+") as f:
        json.dump(data, f)

if __name__ == "__main__":
    main()
