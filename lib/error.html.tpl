<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>Qash</title>
<script>
// Handle events
const socket = new WebSocket(`ws://${location.host}/ws`);
socket.addEventListener("message", function(event) {
  if (event.data === 'reload') {
    location.reload();
  }
});
</script>
</head>
<body>
{{ message }}
</body>
