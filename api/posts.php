<?php
require_once __DIR__ . '/db.php';

$limit  = min((int) ($_GET['limit']  ?? 20), 500);
$offset = max((int) ($_GET['offset'] ?? 0),  0);
$search = trim($_GET['search'] ?? '');

$db = get_db();

if ($search !== '') {
    $stmt = $db->prepare(
        'SELECT p.*, COUNT(c.id) AS comment_count
         FROM posts p
         LEFT JOIN comments c ON c.post_url = p.url
         WHERE p.text LIKE :search
         GROUP BY p.id
         ORDER BY COALESCE(p.posted_at, p.scraped_at) DESC
         LIMIT :limit OFFSET :offset'
    );
    $stmt->bindValue(':search', '%' . $search . '%');
} else {
    $stmt = $db->prepare(
        'SELECT p.*, COUNT(c.id) AS comment_count
         FROM posts p
         LEFT JOIN comments c ON c.post_url = p.url
         GROUP BY p.id
         ORDER BY COALESCE(p.posted_at, p.scraped_at) DESC
         LIMIT :limit OFFSET :offset'
    );
}

$stmt->bindValue(':limit',  $limit,  PDO::PARAM_INT);
$stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
$stmt->execute();

json_response($stmt->fetchAll());
