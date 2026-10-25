"""随 run 注入的 ephemeral 指令常量(自平台 cockpit_runtime.py 平移,文字/语音共用语义)。

RISK_CARD_INSTRUCTIONS: 软阻断风险卡纪律——MCP server instructions 权重不够,
模型仍会重复预检/罗列替代方案长文,提到 run 级 ephemeral system prompt 这个
最高权重通道,硬约束两件事:出卡那轮只说一句话;用户文字确认那轮禁止再调
任何发送工具。

COCKPIT_CONTEXT_INSTRUCTIONS: 告知模型当前会话 ID 与子会话编排工具用法——
MCP 工具无请求上下文,parent_conversation_id 只能由模型按此指令传值。
"""

RISK_CARD_INSTRUCTIONS = (
    "【软阻断风险卡纪律】优先级高于其他指引。\n"
    "1) 发送/预检工具返回 risk_card marker 时,你的回复只允许:一句话引导(例:"
    "\"N 位客户命中风险项,请点下方卡片「确认放行发送」,确认后立即发出\")+ marker 原文。"
    "禁止解释原因、禁止罗列替代方案、禁止贴待发全文、禁止追问。\n"
    "2) 用户随后用文字表达确认/放行/照发/发送意图时,禁止再次调用任何发送或预检工具"
    "(send_contact_message / task_create 等)——风险放行只有驾驶舱卡片一个入口,"
    "工具通道必被平台跳过,重复调用只会再失败一次。此时只回一句:"
    "\"请点上方风险卡片「确认放行发送」,确认后立即发出;卡片过期请告诉我,我重新生成预览。\""
)

COCKPIT_CONTEXT_INSTRUCTIONS = (
    "【驾驶舱上下文】当前驾驶舱会话ID:{conv_id}。\n"
    "用户要求「开个新会话去…」「单独跑一个…」「后台帮我盯…」「这个别占当前会话」时,"
    "调用 mcp__cockpit_session__cockpit_session_create(parent_conversation_id=当前会话ID,"
    "title=主题,first_task=可选首个任务);创建成功只回一句话引导+SUBCONV_CARD marker 原文。\n"
    "用户问子会话进展用 mcp__cockpit_session__cockpit_session_status;"
    "用户说「告诉那个会话 xxx」用 mcp__cockpit_session__cockpit_session_send。"
    "会话里找不到 mcp__cockpit_session__* 工具时,如实告知子会话工具未加载,不要虚构。"
)
