<?php
require_once __DIR__ . '/db.php';

$db = get_db();

$post_count    = (int) $db->query('SELECT COUNT(*) FROM posts')->fetchColumn();
$comment_count = (int) $db->query('SELECT COUNT(*) FROM comments')->fetchColumn();

$last = $db->query(
    'SELECT finished_at, status FROM scrape_log ORDER BY id DESC LIMIT 1'
)->fetch();

json_response([
    'status'               => $last ? $last['status'] : 'never_run',
    'total_posts_scraped'  => $post_count,
    'total_comments'       => $comment_count,
    'last_scrape'          => $last ? $last['finished_at'] : null,
]);
