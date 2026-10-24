# RACE DUO server
Zero dependencies. Node 16+.  `npm start`  |  `npm test` (end-to-end test, 10 checks)

Deploy (any host that gives you HTTPS/WSS): Render / Railway / Fly.io / a VPS behind Caddy or nginx.
Then in Lens Studio set GameController -> serverUrl to  wss://YOUR-APP-DOMAIN
Health check: GET /health

Protocol (JSON text frames)
client->server: create{name} join{code,name} rejoin{token} leave ready{ready} vehicle{id} track{id}
  start state{x,y,vx} pickup boost hit{kind} finish rematch ping{c}
server->client: hello joined lobby start snap(15Hz) correct energy boostOk boostDenied
  finishDenied event peer peerLeft result rematchVote resume error{code} pong{c,s}
Error codes: ROOM_NOT_FOUND ROOM_FULL RACE_IN_PROGRESS WAITING_FOR_PLAYER NOT_READY NOT_IN_ROOM SERVER_FULL
Language is never sent to the server, so each player can use a different language.
