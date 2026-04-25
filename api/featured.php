<?php
require_once __DIR__ . '/db.php';

$db = get_db();

$row = $db->query(
    'SELECT * FROM posts
     WHERE is_cesmii_share = 1
     ORDER BY scraped_at DESC
     LIMIT 1'
)->fetch();

json_response($row ?: null);
