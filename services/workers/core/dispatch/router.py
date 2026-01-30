import random
from datetime import datetime, timedelta
from common.logger import debug_log
from common.models import ConversationRoute, GeminiServiceNode


def get_database_target_url(db, conversation_id, slot_id=0):
    """
    🎯 基于数据库的服务发现逻辑 (分离存储版)
    直接读写 ConversationRoute 表，彻底解决 JSON 覆盖问题。
    """
    try:
        # 1. 查活跃节点 (保持不变)
        alive_threshold = datetime.now() - timedelta(seconds=30)
        active_nodes = db.query(GeminiServiceNode).filter(
            GeminiServiceNode.last_heartbeat > alive_threshold,
            GeminiServiceNode.status == "HEALTHY",
            GeminiServiceNode.dispatched_tasks == 0,
            GeminiServiceNode.current_tasks == 0
        ).all()

        if not active_nodes:
            debug_log("❌ 无可用健康节点", "ERROR")
            return None, False

        healthy_map = {node.node_url: node for node in active_nodes}
        target_url = None

        # =========================================================
        # 🔥 2. 会话粘性 (直接查 ConversationRoute 表)
        # =========================================================
        route_record = None
        if conversation_id:
            # 只查自己槽位的那一行，绝对不会读到别人的 Slot 数据！
            route_record = db.query(ConversationRoute).get((conversation_id, slot_id))

            if route_record:
                last_node_url = route_record.node_url

                # 检查节点是否存活且空闲
                if last_node_url and last_node_url in healthy_map:
                    candidate = healthy_map[last_node_url]
                    if candidate.dispatched_tasks == 0 and candidate.current_tasks == 0:
                        target_url = last_node_url
                        debug_log(f"🔗 [槽位 {slot_id}] 复用节点: {target_url}", "INFO")

        # =========================================================
        # 🔥 3. 负载均衡 & 保存 (直接写 ConversationRoute 表)
        # =========================================================
        if not target_url:
            chosen_node = random.choice(active_nodes)
            target_url = chosen_node.node_url
            debug_log(f"🎲 [槽位 {slot_id}] 新分配: {target_url}", "INFO")

            if conversation_id:
                if route_record:
                    # 如果记录存在，更新它
                    route_record.node_url = target_url
                    # db.add(route_record) # 对象在 session 里，会自动 commit
                else:
                    # 如果记录不存在，创建新行
                    new_route = ConversationRoute(
                        conversation_id=conversation_id,
                        slot_id=slot_id,
                        node_url=target_url
                    )
                    db.add(new_route)

                # 注意：这里我们不立即 commit，而是交给外层 node_manager 统一 commit
                # 这样可以保证 节点锁定 + 路由保存 是一个原子操作

        # 判断是否变更
        is_node_changed = False
        if route_record and route_record.node_url != target_url:
            is_node_changed = True
        elif not route_record:
            is_node_changed = False  # 第一次不算变更，或者你可以算 True

        final_url = f"{target_url}/v1/chat/completions"
        return final_url, is_node_changed

    except Exception as e:
        debug_log(f"❌ 路由异常: {e}", "ERROR")
        return None, False