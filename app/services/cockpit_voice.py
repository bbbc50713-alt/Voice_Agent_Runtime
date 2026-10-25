"""实时语音 DashScope 会话代理(services/cockpit_voice.py)。

用 websockets 裸协议直连 qwen omni realtime(不用 dashscope SDK——SDK 是线程
回调模型,与 FastAPI asyncio 不搭)。一个实例 = 一条 realtime 会话。

契约(docs/REALTIME_VOICE_V2_PLAN.md §2.2/§2.3 + §11 静默闸门 v3.3):
- 建连后立即发 session.update(modalities=text+audio / pcm 16k 进 24k 出 /
  semantic_vad / 业务函数 tools)
- 接收事件统一回调给路由层 on_event(event: dict),内部先维护
  responding/user_speaking 两态与静默闸门再回调
- §11 静默闸门(v3.3):所有系统主动发言(函数回执/任务播报)统一经一个随机
  2~3s 无活动计时器单独起拍;回执(followup)优先于播报,两者隔拍;等待期间
  用户开口/模型起答 → 回执让位(已在上下文不抢话),播报等下个静默窗口;
  打断后转 interrupted 且不立即重排;response 终局经 playback.settle 等浏览器 ack,
  旧客户端由超时兜底;_in_flight_notify/consume/defer 分轨结算并注入【播报进展】
- v3.4: 播报指令不再注入对话流,改经 response.create 的 instructions 本轮
  覆盖下发(模型看不到指令原文,根治复读/泄漏);受理回执附 CONTINUE_PHRASES
  随机邀约开口指引
- v3.5(§14): 旁听模式——instructions 追加 OBSERVE_GUIDANCE,tools 加第 5 函数
  dismiss_turn;模型自当 Address Gate,判定"不是叫我"时调 dismiss_turn 代替
  开口,路由层补 response.cancel 截断残余音频并豁免 followup(总开关
  realtime_voice_observe_enabled,关则恢复有问必答)
- v3.7(播报多样性): 过渡词池扩容+会话级近期去重(_recent_words);ask/direct/tell
  播报指令各建句式变体确定性轮换(_last_variant);directive 追加"说法每次都要
  不一样"要求——根治"逐字相同指令→模型逐字相同播报(差点忘了…)"的复读
- v3.8(防编造复读,任务#29 案例): VOICE_GUIDANCE 明确"对话记录里出现过的业务
  数据(包括模型自己说过的)也不算数,必须调函数以业务后台为准";受理开口禁夹带
  查询对象任何属性(结果没回来前"我查到他是…"都是在猜);结果与先前说法有出入
  时播报/回答必须先明确纠正

活跃会话注册表(模块级): (tenant_id, user_id) → DashScopeVoiceSession,
供 voice_tasks 完成回调查找"该用户当前是否正在语音通话"。
"""

from __future__ import annotations

import asyncio
import base64
import json
import hashlib
import json
import logging
import random
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import websockets

from app.config import get_settings

logger = logging.getLogger(__name__)


# --- instructions(SOUL.md + 语音前台指引段,§2.4/§10/§11) ----------------------------

VOICE_GUIDANCE = """【语音前台工作指引】
1. 你正在语音通话中，回答必须口语化、简短（一两句话先说结论），不要用列表、表格、Markdown。
2. 业务请求一律调函数取数，绝不自己回答业务数据：客户叫什么、什么标签、跟进记录、消费多少、到院没有，只有业务后台知道，你一概不知。即使对话记录里出现过（包括你自己之前说过的）也不准、不算数——用户要"列出来/查一下/看看某客户的情况"时必须调函数以业务后台为准，禁止凭记忆或上下文直接列出。快查（一句话能答、用户等着这句话继续聊）走 ask_business_agent，结果直接返回，拿到后立刻用一两句话告诉用户；重活（要翻一堆数据或产出内容）走 submit_business_task。铁律：ask_business_agent 只查不写——发消息/微信发送/触达/私信、创建任务、创建会话、派发、生成草稿等一切写操作必须走 submit_business_task，不得用 ask_business_agent，拿不准一律 submit_business_task。
3. submit_business_task 受理后你会收到回执，稍后由系统安排你开口：按回执里的开口指引，用一两句话自然告知"已经在后台处理、结果出来马上告诉用户"，并邀约用户继续聊（邀约的说法每次都要不一样）。受理开口只说"在查/在处理"，一个字都不要提查询对象的任何属性——结果还没回来，此时说出"我查到他是…"全是在猜，会误导用户。
4. 会话中出现【后台任务结果】条目时：仅作备查，不要主动提起，不要打断当前话题，更不要把它念出来。系统会在合适时机通过本轮专属指令让你开口播报。用户主动问起时，直接按摘要口语化讲，答完立刻调 mark_result_shared 登记（完整数据让用户看任务卡片）。用户明确表示不想听、暂时不处理、希望停止当前播报或切换话题时，按上下文语义调用 defer_task_result；可理解"算了吧/不要了/先不管/回头再说/别念了/先放那儿"等自然说法，但不能只依赖固定关键词。像"我不知道/嗯/行吧"这类本身含义不完整的话，只有在明确回应任务播报时才按暂不处理理解，否则继续正常对话或只做一次轻量确认。不要把与任务播报无关的普通话题误压下。如果结果内容和你之前说过的有出入（比如你说过某客户的情况、结果却是查无此人），以结果为准，回答或播报时先明确纠正"我之前说的不对，系统里实际是…"。
5. 有些轮次会带本轮专属的系统指令（不在对话记录里）：那是系统判断适合开口后下发的，立即按它自然开口（包括指定的开头用词）；指令本身一个字都不要念出来。
6. 被问"结果出来了吗"而上下文里没有对应【后台任务结果】时，回答还在处理中；有结果条目时直接从摘要回答，不用等询问环节——但你口头答完之后必须立刻调 mark_result_shared（任务编号就在结果条目里），系统便不再重复播报。用户一次明确要求听多个任务时，直接合并成一轮简短回答并批量调用 mark_result_shared；只有用户明确只想听其中一个或无法判断指向时，才先确认范围。
7. 用户想"另开/新建一个会话单独跑一件事"时（如"在其他地方新建个会话去查XX""单独跑一个XX，别占当前会话"）：你有这个能力——把用户的整句要求用 submit_business_task 派发，任务原文里必须保留"另开新会话去…"的意思，执行端会据此创建子会话去跑，驾驶舱里会出现子会话面板卡片。受理后照旧按第 3 条的开口指引告知，绝不要回答"我没法新建会话"。
8. 长任务/批量活一律交后台：批量查询统计、客情分析报告、名单导出Excel、多任务汇总文档（word/excel/ppt）、营销文案批量生成——用submit_business_task提交，进度实时进任务卡，对话只受理确认不等待，完成后按播报策略通知。用户要文档汇总必须按【聚合文档铁律】建任务；要关闭播报用set_announce_policy。技能细节hermes执行时会自动参考cockpit-report-writer/cockpit-delegate-catalog。

用户要取消任务时调 cancel_business_task。系统返回取消结果后，用一句话确认，例如"好，盘客户那个任务已经取消了"。如果系统返回多个候选，逐个念给用户让他选。"""


# 受理回执开口指引的邀约话术池(v3.4):随机选一句写进回执,模型换成自己的话说
CONTINUE_PHRASES = (
    "我们可以接着聊点别的",
    "你还有别的想让我帮忙的吗",
    "咱们接着往下聊",
    "等结果的工夫，聊点别的也行",
    "你先忙着，有进展我马上喊你",
    "顺便想想下一个要盘谁",
)


# 旁听规则段(v3.5,§14):默认旁听、被点名才答;realtime_voice_observe_enabled 开时
# 追加到 instructions 末尾(SOUL + VOICE_GUIDANCE + OBSERVE_GUIDANCE)
# 第二段【对话模式】= ENGAGED_DRIFT_AND_SPEAKER_ATTRIBUTION_PLAN §4.1 Layer 0
# (2026-09-12 第二版,按用户定稿语义调整):叫醒=进入对话模式并一直保持,
# 中途用户和旁人说话只对那一轮沉默(照旧调 dismiss_turn),绝不自己缩回旁听;
# 唯一回旁听出口 = 用户明确叫停。称呼线索规则是防幻觉红线——无声纹能力时
# 绝不根据声音猜身份。
OBSERVE_GUIDANCE = """【旁听规则】你默认在旁听用户工作。以下情况绝不能开口，直接调用 dismiss_turn：
- 用户在和别人说话、讨论事情，问题不是抛给你的；
- 用户自言自语、念资料、打电话；
- 上一句在叫别人的名字（如"小王，你觉得呢"）。
特别注意：dismiss_turn 是静默的函数调用——调用它时你不产生任何语音和文字，
绝不要把"dismiss_turn"这个函数名念出来或写出来，旁听就该像什么都没说。
以下情况正常回答：
- 明确叫你的名字（汉二密/小助手/二密，口音相近也算，如"韩二密""汉尔密"）；
- 问题明显是抛给你的（结合上下文判断"你觉得呢/怎么办"没有指代别人）；
- 用户让你做事（盘客户/查数据/写话术，按原规则调函数）。
被叫到时自然承接上文（你一直在听，知道大家在讨论什么）。

【对话模式】被叫醒（用户叫你名字，或问题明显抛给你）后，进入对话模式，一直保持到用户明确叫停为止：
- 对话模式中，用户说的话默认都是对你说的，正常回应，不需要每句都重新判断"是不是在叫我"。
- 中间用户转头和旁人说了几句、那句明显不是对你说的：只对那一轮调 dismiss_turn 保持沉默即可，
  绝不要因此认为对话结束、更不要借此缩回旁听——下一句只要是用户对你说的，继续正常回应。
  旁人说的话你都听得见、记得住。
- 用户再让你"说说你的看法/你怎么看"时：先用自己的话简要复述刚才的讨论要点
  （上下文里有称呼线索就带名字，如"刚才小王提到售后很关键"；听不出是谁说的，
  就说"刚才那位同事"，绝不硬猜名字），然后再给自己的看法，最后一两句话收住。
- 只有用户明确叫停（"行了/别说了/没你的事了/你忙吧/先这样吧"这类），才回旁听：
  回一句简短的"好，有事叫我"或直接调 dismiss_turn；之后的判门回到旁听标准（叫名字才应）。
  除了这类明确叫停，任何其他理由都不要回旁听。

【称呼线索】引用别人的发言时，只有转写里出现过对方被称呼（如"小王，你说说"）或自我介绍，
才可以用名字；没有任何线索时一律说"那位同事/刚才有人提到"。
你听不出说话人的身份，绝不根据声音猜测是谁在说话。"""


# 专有模式段(spirit-voice-modes-plan §4.1): 声纹锁定本人——后端机械闸门
# (路由层 response.created 时非本人 → response.cancel)兜底,prompt 段负责行为语义:
# 本人免叫名直聊、不回应他人、摘要承接备查。
EXCLUSIVE_GUIDANCE = """【专有模式】本通话开启了声纹专属：系统已注册并锁定用户本人的声音。
- 你只回应用户本人的话；在场其他人说话，系统会在你开口前自动拦截，你不回应他们的内容。
- 本人对你说话时正常对话，不需要每句叫你的名字。
- 会话里的【现场摘要·以最新一条为准】是在场人员发言的系统记录，仅供备查，不要主动提起；
  本人问起讨论内容时按它承接。摘要里写"有人"就照说"有人"，绝不根据声音猜测是谁在说话。"""


# 多人现场段(ENGAGED_DRIFT Layer 1): multi_speaker_enabled 时追加到 instructions 末尾。
# 与【称呼线索】的分工——Layer 0 无声纹时靠称呼线索;Layer 1 有【现场摘要】
# (声纹归属 + 事件账本的实时压缩),名字可用性的唯一依据 = 摘要里写了什么;
# 摘要说"有人"就照说"有人",依然禁止模型自己按声音猜身份。
SIDECAR_GUIDANCE = """【多人现场】本通话开启了多人识别：系统会把在场人员的发言实时整理成【现场摘要·以最新一条为准】条目注入对话，作为你的旁听记忆。
- 摘要是系统对"谁说了什么"的实时记录。被点名发表看法时，按最新一条摘要复述讨论要点并承接：摘要里有名字就带名字（如"刚才小王提到售后很关键"）；摘要写的是"有人提到"就照样说"有人提到"，绝不换成具体名字。
- 摘要仅供你参考：不要主动念出来、不要逐条复述，被问到或被点名时才用。
- 摘要没覆盖的内容你不知道；你依然听不出说话人身份，绝不根据声音猜测是谁在说话。"""


def _default_soul_path() -> Path:
    """独立服务默认人格: <repo>/SOUL.md(realtime_voice_soul_path 可覆盖)。

    repo 根由 app/services/ 上两级推;平台时代默认指向 hermes profile
    SOUL.md,独立服务自带 SOUL.md 副本(部署时按客户品牌替换)。
    """
    backend_dir = Path(__file__).resolve().parents[2]
    return backend_dir / "SOUL.md"


# 显式语音模式(spirit-voice-modes-plan §3.1,WS start.voice_mode): None=legacy 兼容
VOICE_MODES = ("pure", "exclusive", "multi")


def load_voice_instructions(voice_mode: str | None = None) -> str:
    """现读 SOUL.md(realtime_voice_soul_path 或默认 profile 路径)+拼接语音前台指引段。

    每次建连现读不缓存(运营会改 SOUL.md);读不到时降级为仅指引段并 log 警告。
    v3.5: realtime_voice_observe_enabled 开时末尾追加 OBSERVE_GUIDANCE 旁听段。
    v3.6(三模式): voice_mode 显式指定时覆盖全局开关——
      pure=仅前台指引(原始直聊);exclusive=+EXCLUSIVE_GUIDANCE;
      multi=+OBSERVE_GUIDANCE+SIDECAR_GUIDANCE;
      None=legacy(observe/multi_speaker 全局开关拼接,老客户端行为不变)。
    """
    settings = get_settings()
    guidance = VOICE_GUIDANCE
    if voice_mode is None:
        if settings.realtime_voice_observe_enabled:
            guidance = f"{guidance}\n\n{OBSERVE_GUIDANCE}"
        if settings.multi_speaker_enabled:
            guidance = f"{guidance}\n\n{SIDECAR_GUIDANCE}"
    elif voice_mode == "exclusive":
        guidance = f"{guidance}\n\n{EXCLUSIVE_GUIDANCE}"
    elif voice_mode == "multi":
        guidance = f"{guidance}\n\n{OBSERVE_GUIDANCE}\n\n{SIDECAR_GUIDANCE}"
    # pure: 仅 VOICE_GUIDANCE
    soul_path = Path(settings.realtime_voice_soul_path) if settings.realtime_voice_soul_path else _default_soul_path()
    try:
        soul = soul_path.read_text(encoding="utf-8").strip()
    except OSError:
        logger.warning("SOUL.md 读取失败(%s),语音会话降级为仅指引段", soul_path)
        return guidance
    if not soul:
        logger.warning("SOUL.md 为空(%s),语音会话降级为仅指引段", soul_path)
        return guidance
    return f"{soul}\n\n{guidance}"


# --- session.update payload(§2.3 + §11.4, tools = 四函数) ----------------------------

TOOL_ASK_BUSINESS_AGENT: dict[str, Any] = {
    "type": "function",
    "name": "ask_business_agent",
    "description": "只读快查，仅用于能秒回的单一事实查询:某个客户的基本情况/标签/最近跟进、某项数据的总数、单条记录状态。发送消息/微信发送/触达/私信、创建任务、创建会话、派发、生成草稿等一切写操作一律用 submit_business_task，禁止用本函数；盘点筛选一批客户、生成话术、分析总结、批量操作同样一律用 submit_business_task。拿不准就用 submit_business_task。",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "完整的查询问题"},
        },
        "required": ["query"],
    },
}

TOOL_SUBMIT_BUSINESS_TASK: dict[str, Any] = {
    "type": "function",
    "name": "submit_business_task",
    "description": "当用户请求涉及业务数据或业务操作时调用：盘点/筛选客户、查询到院与客情、生成私信/群聊/活动触达话术、总结跟进情况、生成文件(文档/表格)、把多个任务的结果汇总成一份产出、创建或使用技能(skill)完成工作。调用后立即返回，结果会稍后由系统注入会话，届时请主动告知用户。闲聊和通用问答不要调用。【聚合文档铁律】用户要'把几个任务的结果整理成文档/一起汇总/发我一份文件'→必须立即调用本工具:delivery_mode=file + artifact_format按用户要求(word→docx/excel→xlsx/ppt→pptx) + source_task_ids=[全部相关任务id] + announce_policy=file_ready。源任务没跑完也照建(系统自动等待),严禁口头回答'好的稍后整理'而不调用。【播报偏好】用户表达'以后不用播报/别问了'→先调set_announce_policy关在途任务,再新建的任务必须带announce_policy=file_ready或silent_until_group。",
    "parameters": {
        "type": "object",
        "properties": {
            "task": {"type": "string", "description": "用户需求的完整复述，保留所有关键条件"},
            "kind": {"type": "string", "enum": ["盘点", "筛选", "查询", "话术", "总结", "其他"]},
            "delivery_mode": {"type": "string", "enum": ["voice_summary", "task_card", "file"], "description": "交付方式:voice_summary=语音播报摘要(默认);task_card=只出任务卡片;file=生成文件,完成后任务卡里可预览/下载"},
            "artifact_format": {"type": "string", "enum": ["docx", "md", "txt", "xlsx", "csv", "pptx"], "description": "delivery_mode=file 时期望的文件格式"},
            "source_task_ids": {"type": "array", "items": {"type": "integer"}, "description": "聚合场景:要把哪些已有任务的结果汇总进来"},
            "group_intent": {"type": "boolean", "description": "本任务是把多个任务结果汇总的父任务"},
            "announce_policy": {"type": "string", "enum": ["ask", "direct", "file_ready", "silent_until_group"], "description": "播报策略:ask=许可式询问(默认);direct=完成直念;file_ready=文件就绪提示;silent_until_group=静默,等父任务统一播报"},
            "requires_skill": {"type": "boolean", "description": "本任务需要创建或使用 skill 完成"},
            "dedupe_key": {"type": "string", "description": "幂等键:同一请求的重复调用带相同键,后台命中进行中的任务直接复用,不重复受理"},
        },
        "required": ["task"],
    },
}

TOOL_CANCEL_BUSINESS_TASK: dict[str, Any] = {
    "type": "function",
    "name": "cancel_business_task",
    "description": "当用户要求取消/撤销/算了/不用了某个正在受理或执行中的业务任务时调用。已完成或已取消的任务不要调用。",
    "parameters": {
        "type": "object",
        "properties": {
            "task_hint": {
                "type": "string",
                "description": "用户指代任务的只言片语,如'盘客户那个''刚才的话术';用户说'那个任务'时传空串",
            },
        },
    },
}

TOOL_DEFER_TASK_RESULT: dict[str, Any] = {
    "type": "function",
    "name": "defer_task_result",
    "description": "当用户在任务播报上下文里用自然语言表示暂时不想听、不要播、先放一放、算了、回头再说、别念了、别打断或切换话题时调用。不要只匹配固定关键词，要按语义判断；含糊话只有明确回应任务播报时才压下。调用后系统不再主动提起该任务；用户之后主动问起时正常回答。仅对已完成/已失败的任务使用。（持续性偏好如'以后都别播了'用set_announce_policy，本工具只压当前这一批已完成任务）",
    "parameters": {
        "type": "object",
        "properties": {
            "task_hint": {
                "type": "string",
                "description": "指代任务的只言片语,如'盘点那个''刚才的话术';用户说'都先别讲'时传空串表示全部",
            },
            "task_ids": {
                "type": "array",
                "items": {"type": "integer"},
                "description": "明确知道任务编号时传任务编号数组；一次处理多个已完成任务时使用",
            },
            "batch_id": {"type": "string", "description": "可选播报批次编号；当前版本可不传"},
        },
    },
}

# 播报策略批量更新(2026-10-14 对齐平台后端 voice-announce-fix §A): 用户表达持续性偏好时调用
TOOL_SET_ANNOUNCE_POLICY: dict[str, Any] = {
    "type": "function",
    "name": "set_announce_policy",
    "description": "当用户表达持续性播报偏好时调用——'以后都不用播报''别再问我要不要听''全部等文件好了再说''完成后直接给文件不要念'。policy:silent_until_group=全部静默等汇总;file_ready=完成即短通知不再询问;direct=直接播。批量任务/所有在途任务一并生效。defer_task_result 只压当前这一批已完成的任务,本工具管持续性偏好和运行中任务。",
    "parameters": {
        "type": "object",
        "properties": {
            "policy": {
                "type": "string",
                "enum": ["file_ready", "silent_until_group", "direct"],
                "description": "播报策略:file_ready=文件就绪提示;silent_until_group=静默等汇总;direct=直接播",
            },
            "task_ids": {
                "type": "array",
                "items": {"type": "integer"},
                "description": "要更新的任务编号;不传=该用户全部未终态任务",
            },
        },
        "required": ["policy"],
    },
}

# 第 5 函数(方案 rictor-starfire-orphan §B1): 模型口头告知结果后登记"已播报",
# 服务端置 consumed 并摘除播报队列——根治"用户主动问结果→模型答了→播报指令又说一遍"
TOOL_MARK_RESULT_SHARED: dict[str, Any] = {
    "type": "function",
    "name": "mark_result_shared",
    "description": "当你已经口头把一个或多个后台任务的结果告诉用户之后立刻调用（任务编号在【后台任务结果】条目里）。调用后系统不再主动播报这些任务。注意：只说'任务完成了,要不要听'不算已告诉结果；必须在真正讲完结果后调用。",
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {"type": "integer", "description": "【后台任务结果】条目里的任务编号"},
            "task_ids": {
                "type": "array",
                "items": {"type": "integer"},
                "description": "一次讲完多个任务结果时传任务编号数组",
            },
            "batch_id": {"type": "string", "description": "可选播报批次编号；当前版本可不传"},
        },
    },
}

# 第 6 函数(v3.5,§14 旁听模式): 模型自当 Address Gate 的"沉默出口"——
# function call 不产生语音,是 realtime 协议里唯一"行动但不说话"的方式
TOOL_DISMISS_TURN: dict[str, Any] = {
    "type": "function",
    "name": "dismiss_turn",
    "description": "当判定当前这段人声不是叫自己、无需回应时调用（用户在和别人说话/自言自语/打电话/叫别人的名字）。调用后不发出任何声音，继续旁听。只要开口回答就不要调用本函数。",
    "parameters": {
        "type": "object",
        "properties": {},
    },
}


def build_session_update(instructions: str, voice_mode: str | None = None) -> dict[str, Any]:
    """session.update 完整 payload(§2.3;联网搜索与工具互斥,开工具不开搜索)。

    v3.5: realtime_voice_observe_enabled 开时 tools 追加 dismiss_turn(§14)。
    v3.6(三模式): voice_mode='multi' → 追加 dismiss_turn;pure/exclusive → 不追加;
    None=legacy(observe 全局开关决定)。
    常驻 5 函数:ask/submit/cancel/defer/mark_result_shared(方案 rictor-starfire-orphan)。
    2026-10-14: cockpit_announce_policy_enabled 开时追加 set_announce_policy(§A 播报策略)。
    """
    settings = get_settings()
    tools = [
        TOOL_ASK_BUSINESS_AGENT,
        TOOL_SUBMIT_BUSINESS_TASK,
        TOOL_CANCEL_BUSINESS_TASK,
        TOOL_DEFER_TASK_RESULT,
        TOOL_MARK_RESULT_SHARED,
    ]
    if settings.cockpit_announce_policy_enabled:
        tools.append(TOOL_SET_ANNOUNCE_POLICY)
    observe = (voice_mode is None and settings.realtime_voice_observe_enabled) or voice_mode == "multi"
    if observe:
        tools.append(TOOL_DISMISS_TURN)
    return {
        "type": "session.update",
        "session": {
            "modalities": ["text", "audio"],
            "voice": settings.realtime_voice_name,
            "instructions": instructions,
            "audio": {
                "input": {"format": {"type": "pcm", "sample_rate": 16000}},
                "output": {"format": {"type": "pcm", "sample_rate": 24000}},
            },
            "input_audio_transcription": {"model": settings.realtime_voice_asr_model},
            "turn_detection": {
                "type": "semantic_vad",
                "threshold": 0.5,
                "silence_duration_ms": 800,
            },
            "tools": tools,
        },
    }


# --- 工具目录指纹(2026-10-14 防漂移对账) -------------------------------------------------
# 语音域拆分后本文件与平台后端 cockpit_voice.py 各持一份工具目录,曾漂移导致
# "不用播报了/整理成文档"在语音侧完全不可达(2026-10-14 事故)。指纹只覆盖能力面
# (工具名+参数键+枚举+必填),不哈希 description 文案;两侧实现必须逐字一致。

_CATALOG_TOOLS: tuple[dict[str, Any], ...] = (
    TOOL_ASK_BUSINESS_AGENT,
    TOOL_SUBMIT_BUSINESS_TASK,
    TOOL_CANCEL_BUSINESS_TASK,
    TOOL_DEFER_TASK_RESULT,
    TOOL_MARK_RESULT_SHARED,
    TOOL_SET_ANNOUNCE_POLICY,
    TOOL_DISMISS_TURN,
)


def tool_catalog_fingerprint() -> str:
    """工具目录能力面指纹:与平台后端同函数产出一致即未漂移。"""
    spec = []
    for tool in _CATALOG_TOOLS:
        params = tool.get("parameters") or {}
        props = params.get("properties") or {}
        spec.append(
            {
                "name": tool.get("name"),
                "params": sorted(props.keys()),
                "required": sorted(params.get("required") or []),
                "enums": sorted(
                    (k, tuple(v["enum"]))
                    for k, v in props.items()
                    if isinstance(v, dict) and "enum" in v
                ),
            }
        )
    blob = json.dumps(sorted(spec, key=lambda x: str(x["name"])), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


# --- 工具目录防漂移对账慢拍(2026-10-14) ---------------------------------------------------
# 背景:本目录是平台后端 cockpit_voice 工具目录的镜像(双仓 AGENTS.md 约定:改动必须
# 双改+对账测试)。2026-10-14 事故=后端加了 set_announce_policy/聚合参数而镜像漂移,
# 线上模型物理上看不见新能力。本慢拍启动+每 10 分钟与后端指纹端点对账,漂移即
# 落后端系统异常表(voice_tool_catalog_drift)并 warning;对账失败仅跳过,绝不影响
# 任何语音主链路(对齐「全程异常兜底」铁律)。


def check_tool_catalog_drift() -> bool | None:
    """对账一次:本地指纹 vs 平台后端指纹端点。

    返回 True=漂移(已尝试上报),False=一致,None=无法对账(后端未配/不可达/端点异常)。
    """
    from app.config import get_settings

    settings = get_settings()
    base = (settings.platform_api_base or "").rstrip("/")
    token = settings.internal_token
    if not base or not token:
        return None
    local_fp = tool_catalog_fingerprint()
    try:
        import httpx

        resp = httpx.get(
            f"{base}/api/v1/internal/cockpit/voice/tool-catalog-fingerprint",
            headers={"X-Internal-Token": token},
            timeout=5.0,
        )
        if resp.status_code != 200:
            logger.warning("工具目录对账:后端指纹端点 HTTP %s,本次跳过", resp.status_code)
            return None
        remote_fp = str(resp.json().get("fingerprint") or "")
    except Exception:  # noqa: BLE001 - 对账失败只跳过,绝不阻断语音主链路
        logger.warning("工具目录对账:后端不可达,本次跳过", exc_info=True)
        return None
    if remote_fp == local_fp:
        return False
    logger.warning(
        "工具目录漂移:本地(语音侧)=%s 后端=%s,已落后端系统异常表 voice_tool_catalog_drift",
        local_fp,
        remote_fp,
    )
    try:
        import httpx

        httpx.post(
            f"{base}/api/v1/internal/cockpit/voice/tool-catalog-drift",
            headers={"X-Internal-Token": token},
            json={"local_fingerprint": local_fp, "remote_fingerprint": remote_fp},
            timeout=5.0,
        )
    except Exception:  # noqa: BLE001 - 上报失败只留日志
        logger.warning("工具目录漂移上报失败(仅日志,不阻断)", exc_info=True)
    return True


_drift_watch_started = False


def start_tool_catalog_drift_watch() -> None:
    """启动对账慢拍守护线程(幂等,重复调用只生效一次)。由 voice_tasks 启动恢复钩起。"""
    global _drift_watch_started
    if _drift_watch_started:
        return
    from app.config import get_settings

    settings = get_settings()
    if not settings.tool_catalog_drift_enabled:
        return
    _drift_watch_started = True
    interval = max(60, int(settings.tool_catalog_drift_interval_s))

    import threading

    def _loop() -> None:
        while True:
            try:
                check_tool_catalog_drift()
            except Exception:  # noqa: BLE001 - 慢拍自身异常不得杀死守护线程
                logger.exception("工具目录对账慢拍异常")
            threading.Event().wait(interval)

    threading.Thread(target=_loop, name="tool-catalog-drift", daemon=True).start()
    logger.info("工具目录对账慢拍已启动(每 %ss)", interval)


# --- 播报状态机(§11) --------------------------------------------------------------------# noqa: E266

ANNOUNCE_STATES = (
    "pending",
    "queued",
    "announcing",
    "notified",
    "shared",
    "deferred",
    "dismissed",
    "interrupted",
    "consumed",  # legacy: 老数据可读且保持幂等
)


# followup 自由发言轮轻约束: 已问未答/已讲完/已压下/被打断的任务不要重提。
_FOLLOWUP_HINT = (
    "按最新函数回执自然开口（回执里让说什么就说什么，换成自己的话）。"
    "上下文中【播报进展】条目是系统记录：已问未答的任务不要再追问，"
    "已讲完/已压下/被打断的结果不要再念。绝不复读你上一轮已经说过的话。"
)


def _progress_text(task_id: int, titles: dict[int, str], state: str) -> str:
    """【播报进展】条目: 状态流转后注入上下文,让模型知道问过没有/讲完没有。"""
    title = titles.get(task_id, "")
    head = f"【播报进展】任务#{task_id}「{title}」"
    if state == "notified":
        return (
            f"{head}：已问过用户要不要听结果，正在等答复。用户没正面答复前不要再问这件事；"
            "用户问进展时，告知结果已出、确认TA要不要听。"
        )
    if state == "shared":
        return f"{head}：结果已经讲给用户，此事收尾，之后不要再主动提起。"
    if state == "dismissed":
        return f"{head}：结果已放在任务卡片，用户想看自己会看，不要再主动提起。"
    return (
        f"{head}：刚才的播报被用户打断，先别再主动提起；"
        "用户主动问起时再按【后台任务结果】摘要回答。"
    )


# on_announce_transition 回调签名: (task_ids, new_state, bump_attempts) -> Awaitable[None]
AnnounceTransition = Callable[[list[int], str, bool], Awaitable[None]]


# --- 播报指令句式变体(v3.7) --------------------------------------------------------------
# 根因: 同一 mode 只有一份写死模板 → 每次指令逐字相同 → realtime 模型输出逐字相同的
# 播报(「差点忘了,你刚才让我查的…要不要现在听一下」连刷四次)。变体间事实要素完全
# 一致(报名字/不念结果内容/询问或直念),只换修辞结构;_fire_announce 按 mode 确定性
# 轮换(首次=变体0=原模板),配合过渡词池扩容+会话级去重根治复读。
_ASK_VARIANTS = (
    "{names}已完成。请用“{word}”开头，主动告知用户这些任务已完成，"
    "逐个报名字，并询问是否现在听结果。不要念结果内容；"
    "用户说要听，再从上下文【后台任务结果】里讲；若结果与你之前说过的有出入，以结果为准并主动纠正。",
    "{names}已完成。请用“{word}”开头，告诉用户之前交代的事已经出结果了，"
    "点一下是哪几件事，问用户想现在听还是待会再听。不要念结果内容；"
    "用户要听，再从上下文【后台任务结果】里讲；若结果与你之前说过的有出入，以结果为准并主动纠正。",
    "{names}已完成。请用“{word}”开头，顺带提一句这几个活都跑完了，"
    "语气轻松自然，问用户要不要听听结果。不要念结果内容；"
    "用户要听，再从上下文【后台任务结果】里讲；若结果与你之前说过的有出入，以结果为准并主动纠正。",
    "{names}已完成。请用“{word}”开头，说东西都给用户备好了，"
    "报出任务名字，问现在要不要过一遍。不要念结果内容；"
    "用户要听，再从上下文【后台任务结果】里讲；若结果与你之前说过的有出入，以结果为准并主动纠正。",
)
_DIRECT_VARIANTS = (
    "{names}已完成。请用“{word}”开头，直接把结论用一两句话"
    "告诉用户（从上下文【后台任务结果】的摘要里讲；若与你之前说过的有出入，先纠正再讲），完整数据提示用户看任务卡片。",
    "{names}已完成。请用“{word}”开头，开门见山把结论讲给用户"
    "（从上下文【后台任务结果】的摘要里讲；若与你之前说过的有出入，先纠正再讲），一两句话说完，完整数据让用户看任务卡片。",
    "{names}已完成。请用“{word}”开头，用转述的口气把结果带给用户"
    "（依据上下文【后台任务结果】的摘要；若与你之前说过的有出入，先纠正再讲），末尾提一句完整数据在任务卡片里。",
)
_TELL_VARIANTS = (
    "{names}没跑成。请用“{word}”开头，自然告知用户任务没跑成、"
    "可以稍后再试，语气自然，不要念错误码。",
    "{names}没跑成。请用“{word}”开头，跟用户说声抱歉，这件事没办成，"
    "让用户想重试的时候说一声，不要念错误码。",
    "{names}没跑成。请用“{word}”开头，如实说这个任务失败了、没拿到结果，"
    "问用户要不要再试一次，不要念错误码。",
)


def _fmt_task_names(titles: dict[int, str], ids: list[int], merge_max: int) -> str:
    """播报点名列表: 封顶 merge_max,超出归并为"另外还有 N 个任务"。"""
    named, overflow = ids[:merge_max], ids[merge_max:]
    names = "、".join(f"任务#{t}「{titles.get(t, '')}」" for t in named)
    if overflow:
        names += f"，另外还有 {len(overflow)} 个任务"
    return names


# --- DashScope 会话 -------------------------------------------------------------------


class DashScopeVoiceSession:
    """一条 qwen omni realtime 会话(websockets 直连,asyncio 原生)。

    状态跟踪: responding(response.created/done)、user_speaking(speech_started/
    stopped);§11 静默闸门(v3.3): announce_queue(待播报队列)/_followup_pending
    (函数回执待开口)共用一个随机延时计时器 _proactive_timer,系统主动发言一律
    静默 2~3s 后单独起拍;_in_flight_notify(询问送达→notified)/
    _in_flight_consume(结果直告送达→shared)/_in_flight_defer(收尾告知→dismissed)。
    """

    def __init__(
        self,
        *,
        instructions: str,
        on_event: Callable[[dict], Awaitable[None]],
        voice_mode: str | None = None,
    ) -> None:
        self.instructions = instructions
        self.on_event = on_event
        self.voice_mode = voice_mode
        self.responding = False
        self.user_speaking = False
        # §11 静默闸门状态
        self.announce_queue: deque[int] = deque()
        self._in_flight_notify: set[int] = set()
        self._in_flight_consume: set[int] = set()
        self._in_flight_defer: set[int] = set()
        # 播放回执结算: response 终局先挂起,等浏览器 ack 或超时再结算
        self._playback_seq = 0
        self._pending_settle: dict[int, tuple[set[int], set[int], set[int], bool]] = {}
        self._settle_timers: dict[int, asyncio.TimerHandle] = {}
        self._attempts: dict[int, int] = {}
        self._mode: dict[int, str] = {}  # task_id → "ask"(许可式) | "direct"(快任务直念) | "tell"(失败直告)
        self._titles: dict[int, str] = {}
        self._words: dict[int, str] = {}  # task_id → 播报指令指定的开头过渡词
        # v3.7 播报多样性记忆(纯会话内存): 最近播报用过的过渡词(去重) + 各 mode 上次句式变体索引
        self._recent_words: deque[str] = deque(maxlen=2)
        self._last_variant: dict[str, int] = {}
        self._injected_ids: set[int] = set()
        self._followup_pending = False  # 函数回执待开口(让位语义:用户/模型先动则取消)
        self._proactive_timer: asyncio.TimerHandle | None = None
        # 系统主动发言追踪(§11 静默闸门触发的回执/播报): 专有模式机械闸门据此豁免——
        # 闸门管的是"模型不回应非本人说的话",系统主动开口不是回应任何人,不应被误杀
        self._proactive_pending = False  # 已发起 create_response,等 response.created 确认
        self._proactive_active = False  # response.created 已确认本轮是系统主动发言
        # 状态流转回调(voice_tasks.apply_announce_transition,路由层接线)
        self.on_announce_transition: AnnounceTransition | None = None
        self.on_task_event: Any = None
        self._ws: Any = None
        self._recv_task: asyncio.Task | None = None
        self._closed = False

    async def connect(self) -> None:
        """建连 → 发 session.update → 启动接收循环(事件交给 on_event)。"""
        settings = get_settings()
        if not settings.dashscope_realtime_api_key:
            raise RuntimeError("dashscope_realtime_api_key 未配置")
        url = f"{settings.dashscope_realtime_url}?model={settings.dashscope_realtime_model}"
        self._ws = await websockets.connect(
            url,
            additional_headers={
                "Authorization": f"Bearer {settings.dashscope_realtime_api_key}"
            },
            open_timeout=20,  # 实测阿里云端到端握手 5~10s+,默认 10s 会误杀慢网
        )
        await self._send(build_session_update(self.instructions, self.voice_mode))
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def _recv_loop(self) -> None:
        try:
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    continue  # 服务端只发 JSON 文本帧
                try:
                    event = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(event, dict):
                    continue
                await self._track_state(event)
                await self.on_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 断连/解析异常都视为会话结束
            logger.warning("dashscope realtime 接收循环异常结束", exc_info=True)
        finally:
            if self._ws is not None:
                logger.info(
                    "dashscope realtime 会话结束: close_code=%s close_reason=%r",
                    self._ws.close_code,
                    self._ws.close_reason,
                )
            self._closed = True
            self._cancel_proactive_timer()
            try:
                await self.on_event({"type": "closed"})
            except Exception:  # noqa: BLE001
                logger.warning("closed 事件回调失败", exc_info=True)

    # --- §11 静默闸门 ---------------------------------------------------------------

    async def request_announce(
        self,
        task_id: int,
        text: str,
        *,
        mode: str = "ask",
        title: str = "",
        attempts: int = 0,
        word: str = "",
        queue: bool = True,
    ) -> None:
        """voice_tasks 完成/补注回调: 注入文本 + 登记静默闸门播报调度。

        - mode="ask"(成功慢任务): 入队等静默窗口,许可式询问是否听结果;
        - mode="direct"(快任务): 同样入队等静默窗口,但播报直接念结论不询问;
        - mode="tell"(失败任务): 入队等静默窗口,直告没跑成不询问;
        - queue=False(deferred 重连静默补注): 只注入作上下文备查,不登记任何调度。
        """
        if task_id not in self._injected_ids:
            await self.inject_text(text)
            self._injected_ids.add(task_id)
        self._titles[task_id] = title[:30]
        if not queue:
            return
        self._attempts[task_id] = attempts
        self._mode[task_id] = mode
        self._words[task_id] = word
        if task_id not in self.announce_queue:
            self.announce_queue.append(task_id)
        self._maybe_schedule()

    def schedule_followup(self) -> None:
        """函数回执后登记一次跟随发言(v3.3): 静默 2~3s 后由系统触发模型说出回执。

        等待期间任何活动(用户开口/模型起答)都会取消本次触发——回执已在上下文,
        模型答用户时自然涵盖,不抢话。
        """
        self._followup_pending = True
        self._maybe_schedule()

    def defer_tasks(self, task_ids: list[int]) -> None:
        """voice_tasks defer_task_result 调用: 从全部调度结构摘除(defer 优先于重排)。"""
        drop = set(task_ids)
        for tid in drop:
            self._in_flight_notify.discard(tid)
            self._in_flight_consume.discard(tid)
            self._in_flight_defer.discard(tid)
            self._attempts.pop(tid, None)
            self._mode.pop(tid, None)
            self._words.pop(tid, None)
        if drop:
            self.announce_queue = deque(t for t in self.announce_queue if t not in drop)

    def consume_tasks(self, task_ids: list[int]) -> None:
        """mark_result_shared 调用: 模型已口头告知结果,从全部调度结构摘除(同 defer 语义,
        防静默窗口到点重复播报;方案 rictor-starfire-orphan §B1)。"""
        self.defer_tasks(task_ids)

    @property
    def current_response_proactive(self) -> bool:
        """当前(或紧接着将要创建)的 response 是否系统主动发言(回执/播报)。

        路由层专有闸门判据: True 时 response.created 不过闸(没有触发用户,判"当前说话人"
        毫无意义,只会拿环境声/陈旧判定误杀系统播报)。
        """
        return self._proactive_pending or self._proactive_active

    def _cancel_proactive_timer(self) -> None:
        if self._proactive_timer is not None:
            self._proactive_timer.cancel()
            self._proactive_timer = None

    def _maybe_schedule(self) -> None:
        """空闲判定 + 武装静默闸门;等待源=函数回执(followup)+播报队列(§11 配置)。"""
        self._cancel_proactive_timer()
        if self._closed or self.responding or self.user_speaking:
            return
        if not self._followup_pending and not self.announce_queue:
            return
        settings = get_settings()
        if self._followup_pending:
            lo = settings.realtime_voice_followup_delay_min_s
            hi = settings.realtime_voice_followup_delay_max_s
        else:
            has_retry = any(self._attempts.get(tid, 0) > 0 for tid in self.announce_queue)
            if has_retry:
                lo = settings.realtime_voice_announce_idle_retry_min_s
                hi = settings.realtime_voice_announce_idle_retry_max_s
            else:
                lo = settings.realtime_voice_announce_idle_first_min_s
                hi = settings.realtime_voice_announce_idle_first_max_s
        delay = random.uniform(min(lo, hi), max(lo, hi))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._proactive_timer = loop.call_later(
            delay, lambda: asyncio.create_task(self._fire_proactive())
        )

    async def _fire_proactive(self) -> None:
        """静默窗口到点: followup 回执优先开口,播报队列等下一拍(两句话隔开)。"""
        self._proactive_timer = None
        if self._closed or self.responding or self.user_speaking:
            return  # 事件会重新调度
        if self._followup_pending:
            self._followup_pending = False
            try:
                self._proactive_pending = True
                await self.create_response(instructions=_FOLLOWUP_HINT)
            except Exception:  # noqa: BLE001 - 与服务端响应竞态只记日志
                self._proactive_pending = False  # 发起失败复位,防污染下一轮用户响应
                logger.warning("followup response.create 失败", exc_info=True)
            if self.announce_queue:
                self._maybe_schedule()  # 播报等下一个静默窗口,与回执隔开一拍
            return
        await self._fire_announce()

    async def _fire_announce(self) -> None:
        """静默窗口到点: 许可询问(ask)/快任务直念(direct)/直告失败(tell)/收尾转存(close)。"""
        if self._closed or self.responding or self.user_speaking:
            return  # 事件会重新调度
        if not self.announce_queue:
            return
        settings = get_settings()
        max_attempts = settings.realtime_voice_announce_max_attempts
        ask_ids: list[int] = []
        direct_ids: list[int] = []
        tell_ids: list[int] = []
        close_ids: list[int] = []
        while self.announce_queue:
            tid = self.announce_queue.popleft()
            if self._mode.get(tid) == "tell":
                tell_ids.append(tid)
            elif self._mode.get(tid) == "direct":
                direct_ids.append(tid)
            elif self._attempts.get(tid, 0) >= max_attempts:
                close_ids.append(tid)
            else:
                ask_ids.append(tid)
        if not (ask_ids or direct_ids or tell_ids or close_ids):
            return

        merge_max = settings.realtime_voice_announce_merge_max
        # 局部导入: voice_tasks 模块级反向依赖本模块(cockpit_voice),顶层导入会循环
        from app.services import voice_tasks

        def _word_of(ids: list[int]) -> str:
            """选过渡词(v3.7): 优先用 voice_tasks 选好的;若在本会话近期用过则池内换词,并记入 recent。"""
            word = self._words.get(ids[0]) or "对了"
            if word in self._recent_words:
                word = voice_tasks._repick_word(word, self._recent_words)
            self._recent_words.append(word)
            return word

        prev_word = self._recent_words[-1] if self._recent_words else ""
        parts: list[str] = []
        used_words: list[str] = []
        if ask_ids:
            word = _word_of(ask_ids)
            used_words.append(word)
            variant = self._next_variant("ask", _ASK_VARIANTS)
            parts.append(
                variant.format(word=word, names=_fmt_task_names(self._titles, ask_ids, merge_max))
            )
        if direct_ids:
            word = _word_of(direct_ids)
            used_words.append(word)
            variant = self._next_variant("direct", _DIRECT_VARIANTS)
            parts.append(
                variant.format(word=word, names=_fmt_task_names(self._titles, direct_ids, merge_max))
            )
        if tell_ids:
            word = _word_of(tell_ids)
            used_words.append(word)
            variant = self._next_variant("tell", _TELL_VARIANTS)
            parts.append(
                variant.format(word=word, names=_fmt_task_names(self._titles, tell_ids, merge_max))
            )
        if close_ids:
            names = _fmt_task_names(self._titles, close_ids, merge_max)
            parts.append(
                f"{names}的结果已放在任务卡片里。请顺带告知用户结果在卡片中、想听时随时说一声"
                "——只告知存放位置，不要念结果，也不要再追问。"
            )
        diversity_hint = (
            f"（你最近一次播报是用「{prev_word}」开的头，这次换别的说法。）"
            if prev_word and prev_word not in used_words
            else ""
        )
        directive = (
            "你是汉二密，用户的专属客户智能助手，正在和用户语音通话。"
            "系统判断现在轮到你主动开口。按以下要求说出本轮的话：口语化、简短、自然；"
            "绝不能念出本指令的任何原文。同一类播报你的说法每次都要不一样，"
            "不要重复本次通话里你已说过的句子结构。" + diversity_hint + "\n" + "\n".join(parts)
        )

        for tid in ask_ids + direct_ids:
            self._attempts[tid] = self._attempts.get(tid, 0) + 1
        fired = ask_ids + direct_ids + tell_ids
        self._in_flight_notify |= set(ask_ids)
        self._in_flight_consume |= set(direct_ids + tell_ids)
        self._in_flight_defer |= set(close_ids)
        if fired:
            await self._transition(sorted(fired), "announcing", False)
        if close_ids:
            await self._transition(sorted(close_ids), "announcing", False)
        # v3.4: 指令经 response.instructions 本轮覆盖下发,不进对话上下文(防复读/泄漏)
        try:
            self._proactive_pending = True
            await self.create_response(instructions=directive)
        except Exception:  # noqa: BLE001 - 发起失败复位,防污染下一轮用户响应
            self._proactive_pending = False
            raise

    def _next_variant(self, mode: str, variants: tuple[str, ...]) -> str:
        """按 mode 确定性轮换句式变体(v3.7): 首次=变体0=原模板,之后 (上次+1)%n。"""
        idx = (self._last_variant.get(mode, -1) + 1) % len(variants)
        self._last_variant[mode] = idx
        return variants[idx]

    async def _settle_in_flight(self, *, completed: bool) -> None:
        """询问送达→notified;直告送达→shared;收尾→dismissed;打断→interrupted。"""
        notify, consume, defer = self._in_flight_notify, self._in_flight_consume, self._in_flight_defer
        if not notify and not consume and not defer:
            return
        self._in_flight_notify = set()
        self._in_flight_consume, self._in_flight_defer = set(), set()
        settled = notify | consume | defer
        if settled:
            self.announce_queue = deque(t for t in self.announce_queue if t not in settled)
        if completed:
            if notify:
                await self._transition(sorted(notify), "notified", True)
            if consume:
                for tid in consume:
                    self._attempts[tid] = self._attempts.get(tid, 0) + 1
                await self._transition(sorted(consume), "shared", True)
            if defer:
                await self._transition(sorted(defer), "dismissed", False)
            await self._inject_progress(notify, "notified")
            await self._inject_progress(consume, "shared")
            await self._inject_progress(defer, "dismissed")
            return
        # 被打断后用户优先,不立即重排抢话;任务卡保留结果,用户可主动问起。
        await self._transition(sorted(settled), "interrupted", False)
        await self._inject_progress(settled, "interrupted")

    async def _defer_settle(self, *, completed: bool) -> None:
        """向浏览器发 playback.settle,等真实播放 ack 或超时再结算。"""
        notify, consume, defer = (
            self._in_flight_notify,
            self._in_flight_consume,
            self._in_flight_defer,
        )
        if not notify and not consume and not defer:
            return
        self._in_flight_notify = set()
        self._in_flight_consume, self._in_flight_defer = set(), set()
        self._playback_seq += 1
        seq = self._playback_seq
        self._pending_settle[seq] = (notify, consume, defer, completed)
        sink = self.on_task_event
        if sink is not None:
            try:
                await sink({"type": "playback.settle", "seq": seq})
            except Exception:  # noqa: BLE001 - 推送失败由回退定时器兜底
                logger.warning("playback.settle 帧推送失败(seq=%s)", seq, exc_info=True)
        timeout_s = get_settings().realtime_voice_playback_ack_timeout_s
        loop = asyncio.get_running_loop()
        self._settle_timers[seq] = loop.call_later(
            timeout_s,
            lambda s=seq: asyncio.ensure_future(self._settle_seq(s, interrupted=False)),
        )

    async def ack_playback(self, seq: int, *, interrupted: bool) -> None:
        """浏览器播放完成/打断回执;迟到、重复或未知 seq 静默忽略。"""
        await self._settle_seq(seq, interrupted=interrupted)

    async def _settle_seq(self, seq: int, *, interrupted: bool) -> None:
        pending = self._pending_settle.pop(seq, None)
        if pending is None:
            return
        timer = self._settle_timers.pop(seq, None)
        if timer is not None:
            timer.cancel()
        notify, consume, defer, completed = pending
        self._in_flight_notify |= notify
        self._in_flight_consume |= consume
        self._in_flight_defer |= defer
        await self._settle_in_flight(completed=completed and not interrupted)

    async def _inject_progress(self, task_ids: set[int] | list[int], state: str) -> None:
        """结算后注入【播报进展】;注入失败只 log,不影响状态结算。"""
        for tid in sorted(task_ids):
            try:
                await self.inject_text(_progress_text(tid, self._titles, state))
            except Exception:  # noqa: BLE001
                logger.warning("播报进展注入失败(task=%s, state=%s)", tid, state, exc_info=True)

    async def _transition(self, task_ids: list[int], state: str, bump_attempts: bool) -> None:
        cb = self.on_announce_transition
        if cb is None or not task_ids:
            return
        try:
            await cb(task_ids, state, bump_attempts)
        except Exception:  # noqa: BLE001 - 状态落库失败不阻断语音主链路
            logger.warning("播报状态流转失败(%s→%s)", task_ids, state, exc_info=True)

    # --- 事件状态跟踪 -----------------------------------------------------------------

    async def _track_state(self, event: dict) -> None:
        """§11: responding/user_speaking 两态 + 结算/静默闸门联动。"""
        event_type = event.get("type")
        if event_type == "response.created":
            self.responding = True
            # 系统主动发言确认(pending→active);用户触发响应两标志均为 False
            self._proactive_active = self._proactive_pending
            self._proactive_pending = False
            self._cancel_proactive_timer()
            self._followup_pending = False  # 模型已起答(用户驱动),回执让位
        elif event_type == "response.done":
            self.responding = False
            self._proactive_active = False
            self._proactive_pending = False
            status = str((event.get("response") or {}).get("status") or "completed")
            await self._defer_settle(completed=(status == "completed"))
            self._maybe_schedule()
        elif event_type == "response.cancelled":
            self._proactive_active = False
            self._proactive_pending = False
            await self._defer_settle(completed=False)
        elif event_type == "input_audio_buffer.speech_started":
            self.user_speaking = True
            self._cancel_proactive_timer()
            self._followup_pending = False  # 用户开口,回执让位(已在上下文)
            if self.responding and self.current_response_proactive:
                try:
                    await self.cancel_response()
                except Exception:  # noqa: BLE001 - 打断兜底,不影响主接收循环
                    logger.warning("主动播报打断取消失败", exc_info=True)
        elif event_type == "input_audio_buffer.speech_stopped":
            self.user_speaking = False
            self._maybe_schedule()

    # --- 发送原语 ----------------------------------------------------------------------

    async def _send(self, payload: dict) -> None:
        if self._ws is None:
            raise RuntimeError("dashscope realtime 会话未连接")
        await self._ws.send(json.dumps(payload, ensure_ascii=False))

    async def send_audio(self, pcm: bytes) -> None:
        """浏览器二进制音频块 → input_audio_buffer.append(base64 包装)。"""
        await self._send(
            {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(pcm).decode("ascii"),
            }
        )

    async def send_function_output(self, call_id: str, output: str) -> None:
        """conversation.item.create(function_call_output):函数调用回执。"""
        await self._send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": output,
                },
            }
        )

    async def inject_text(self, text: str) -> None:
        """conversation.item.create(input_text):注入【后台任务结果】等系统条目(§2.5)。"""
        await self._send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text}],
                },
            }
        )

    async def create_response(self, instructions: str | None = None) -> None:
        """response.create:触发一轮回答。

        v3.4: 带 instructions 时本轮按指令开口(response.instructions 本轮覆盖,
        DashScope SDK 官方支持)——指令不进对话上下文,杜绝模型复读指令原文。
        """
        payload: dict[str, Any] = {"type": "response.create"}
        if instructions:
            payload["response"] = {"instructions": instructions}
        await self._send(payload)

    async def cancel_response(self) -> None:
        """response.cancel:截断进行中的回答(v3.5 dismiss_turn 配套,SDK cancel_response 同款)。"""
        await self._send({"type": "response.cancel"})

    async def close(self) -> None:
        """断连收尾;幂等,路由层 try/finally 铁律的清理终点。

        注意:实测 qwen3.5-omni-plus-realtime 拒绝 session.finish 事件
        ('Invalid value: session.finish'),官方文档允许的"直接断开连接"才是正确做法。
        """
        if self._closed and self._ws is None:
            return
        self._closed = True
        self._cancel_proactive_timer()
        # 断连时未决播放结算立即按原状态收尾,并取消全部回退定时器。
        for seq in list(self._pending_settle):
            try:
                await self._settle_seq(seq, interrupted=False)
            except Exception:  # noqa: BLE001 - 收尾兜底,绝不让定时器泄漏
                logger.warning("断连结算 seq=%s 失败", seq, exc_info=True)
                self._pending_settle.pop(seq, None)
                timer = self._settle_timers.pop(seq, None)
                if timer is not None:
                    timer.cancel()
        try:
            if self._ws is not None:
                await self._ws.close()
        finally:
            self._ws = None
            if self._recv_task is not None:
                self._recv_task.cancel()
                self._recv_task = None


# --- 活跃语音会话注册表(模块级) ----------------------------------------------------------

_active_voice_sessions: dict[tuple[int, int], DashScopeVoiceSession] = {}


def register_voice_session(
    tenant_id: int, user_id: int, session: DashScopeVoiceSession
) -> None:
    _active_voice_sessions[(int(tenant_id), int(user_id))] = session


def unregister_voice_session(
    tenant_id: int, user_id: int, session: DashScopeVoiceSession | None = None
) -> None:
    """注销;传 session 时仅当注册表里仍是同一实例才删(避免误删新连接)。"""
    key = (int(tenant_id), int(user_id))
    if session is None:
        _active_voice_sessions.pop(key, None)
    elif _active_voice_sessions.get(key) is session:
        _active_voice_sessions.pop(key, None)


def get_active_voice_session(
    tenant_id: int, user_id: int
) -> DashScopeVoiceSession | None:
    return _active_voice_sessions.get((int(tenant_id), int(user_id)))
