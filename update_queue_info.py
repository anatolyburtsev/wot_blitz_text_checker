import config
import sql_result_api
import logic
import logging


logging.debug("start updating info about posts in queue")
posts_count_info = logic.how_many_posts_in_queue(config.vk_group)
sql_result_api.save_to_db(posts_count_info)
logging.debug("finish updating info about posts in queue")