<?php
require_once __DIR__ . '/db.php';

$limit  = min((int) ($_GET['limit']  ?? 50), 500);
$offset = max((int) ($_GET['offset'] ?? 0),  0);

$db = get_db();

$stmt = $db->prepare(
    'SELECT c.id,
            p.url              AS post_url,
            p.author           AS post_author,
            p.author_avatar    AS post_author_avatar,
            p.author_headline  AS post_author_headline,
            p.text             AS post_text,
            p.image_url        AS post_image_url,
            p.post_age,
            c.comment_author,
            c.comment_text,
            c.scraped_at
     FROM comments c
     JOIN posts p ON c.post_url = p.url
     ORDER BY c.scraped_at DESC
     LIMIT :limit OFFSET :offset'
);
$stmt->bindValue(':limit',  $limit,  PDO::PARAM_INT);
$stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
$stmt->execute();

json_response($stmt->fetchAll());
