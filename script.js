class MQTTeam {
  constructor() {
    this.nickname = '';
    this.mqttClient = null;
    this.isConnected = false;
    this.messages = [];
    this.activeUsers = new Set(['admin', 'user']);

    // 환경에 맞게 조정
    this.API_BASE = 'http://192.168.0.33:3000/api';
    this.MQTT_BROKER = 'ws://192.168.0.33:28083/mqtt';
    this.MQTT_TOPICS = {
      PUBLIC: 'k8s-chat/public',
      REACTION: 'k8s-chat/reaction'
    };

    this.initEventListeners();
    this.loadPreviousMessages();
  }

  initEventListeners() {
    // 로그인
    const joinBtn = document.getElementById('joinBtn');
    const nicknameInput = document.getElementById('nicknameInput');
    const leaveBtn = document.getElementById('leaveBtn');

    if (joinBtn) joinBtn.addEventListener('click', () => this.handleJoin());
    if (nicknameInput) {
      nicknameInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') this.handleJoin();
      });
    }
    if (leaveBtn) leaveBtn.addEventListener('click', () => this.handleLeave());

    // 메시지 전송
    const sendBtn = document.getElementById('sendBtn');
    const messageInput = document.getElementById('messageInput');
    const mentionBtn = document.getElementById('mentionBtn');

    if (sendBtn) sendBtn.addEventListener('click', () => this.sendMessage());
    if (messageInput) {
      messageInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          this.sendMessage();
        }
      });
      messageInput.addEventListener('input', () => {
        messageInput.style.height = 'auto';
        messageInput.style.height = messageInput.scrollHeight + 'px';
      });
    }
    if (mentionBtn && messageInput) {
      mentionBtn.addEventListener('click', () => {
        messageInput.focus();
        messageInput.value += '@';
      });
    }

    // 사이드바 토글 (헤더의 버튼 하나만 사용)
    const sidebar = document.getElementById('sidebar');
    const toggleSidebarBtn = document.getElementById('toggleSidebarBtn');

    if (sidebar && toggleSidebarBtn) {
      toggleSidebarBtn.addEventListener('click', () => {
        const isHidden = sidebar.classList.toggle('hidden');

        // 아이콘 교체
        const icon = toggleSidebarBtn.querySelector('img');
        if (isHidden) {
          icon.src = 'Show-user.png';
          icon.alt = '펼치기';
          toggleSidebarBtn.setAttribute('aria-label', '참여자 패널 펼치기');
        } else {
          icon.src = 'Hide-user.png';
          icon.alt = '접기';
          toggleSidebarBtn.setAttribute('aria-label', '참여자 패널 접기');
        }
      });
    }
  }

  async loadPreviousMessages() {
    try {
      const response = await fetch(`${this.API_BASE}/messages`);
      const result = await response.json();
      if (result.status === 'success' && result.data) {
        this.messages = result.data;
        this.renderMessages();
      } else {
        this.useMockMessages();
      }
    } catch {
      this.useMockMessages();
    }
  }

  useMockMessages() {
    this.messages = [{
      id: 1,
      nickname: 'admin',
      content: 'MQTTeam 채팅 서버에 오신 것을 환영합니다.',
      created_at: new Date().toISOString(),
      reactions: {},
      mentions: []
    }];
    this.renderMessages();
  }

  async handleJoin() {
    const nicknameInput = document.getElementById('nicknameInput');
    const joinBtn = document.getElementById('joinBtn');
    if (!nicknameInput) return;

    const nickname = nicknameInput.value.trim();
    if (!nickname) return this.showError('닉네임을 입력해주세요');
    if (nickname.length < 2 || nickname.length > 20)
      return this.showError('닉네임은 2-20자로 입력해주세요');

    if (joinBtn) {
      joinBtn.disabled = true;
      joinBtn.textContent = '연결 중...';
    }

    try {
      const response = await fetch(`${this.API_BASE}/join`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nickname })
      });

      if (!response.ok) throw new Error('사용 중인 닉네임입니다');

      this.nickname = nickname;

      await this.connectMQTT();
      this.showChatInterface();

    } catch (e) {
      this.showError(e.message || '연결 실패');
    } finally {
      if (joinBtn) {
        joinBtn.disabled = false;
        joinBtn.textContent = 'Join Chat';
      }
    }
  }

  showError(msg) {
    const errorElement = document.getElementById('errorMessage');
    if (!errorElement) return;
    errorElement.textContent = msg;
    errorElement.style.display = 'block';
    setTimeout(() => errorElement.style.display = 'none', 3000);
  }

  async connectMQTT() {
    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(this.MQTT_BROKER);

      this.mqttClient.on('connect', () => {
        this.isConnected = true;
        this.updateConnectionStatus();
        this.mqttClient.subscribe(
          [this.MQTT_TOPICS.PUBLIC, this.MQTT_TOPICS.REACTION],
          { qos: 1 },
          (err) => err ? reject(err) : resolve()
        );
      });

      this.mqttClient.on('message', (topic, message) => {
        const data = JSON.parse(message.toString());
        if (topic === this.MQTT_TOPICS.PUBLIC) this.handleIncomingMessage(data);
        if (topic === this.MQTT_TOPICS.REACTION) this.handleIncomingReaction(data);
      });

      this.mqttClient.on('error', (err) => reject(err));
    });
  }

  showChatInterface() {
    document.getElementById('loginContainer').style.display = 'none';
    document.getElementById('chatContainer').style.display = 'flex';
    this.renderMessages();
    this.renderUserList();
  }

  sendMessage() {
    const messageInput = document.getElementById('messageInput');
    if (!messageInput) return;
    const content = messageInput.value.trim();
    if (!content || !this.mqttClient?.connected) return;

    const message = {
      id: Date.now(),
      nickname: this.nickname,
      content,
      created_at: new Date().toISOString(),
      reactions: {},
      mentions: this.parseMentions(content)
    };

    this.mqttClient.publish(this.MQTT_TOPICS.PUBLIC, JSON.stringify(message), { qos: 1 });
    messageInput.value = '';
    messageInput.style.height = 'auto';
  }

  parseMentions(content) {
    return Array.from(content.matchAll(/@(\w+)/g)).map(m => m[1]);
  }

  handleIncomingMessage(msg) {
    this.messages.push(msg);
    this.renderMessages();
  }

  handleIncomingReaction(data) {
    const { messageId, reaction, nickname } = data;
    const msg = this.messages.find(m => m.id === messageId);
    if (!msg) return;

    for (let key in msg.reactions) {
      msg.reactions[key] = msg.reactions[key].filter(n => n !== nickname);
    }
    if (!(msg.reactions[reaction] && msg.reactions[reaction].includes(nickname))) {
      msg.reactions[reaction] = msg.reactions[reaction] || [];
      msg.reactions[reaction].push(nickname);
    }
    this.renderMessages();
  }

  renderMessages() {
    const container = document.getElementById('messagesContainer');
    if (!container) return;

    container.innerHTML = this.messages.map(msg => {
      const isOwn = msg.nickname === this.nickname;
      return `
        <div class="message ${isOwn ? 'own' : ''}" data-id="${msg.id}">
          <div class="message-content">
            <div class="message-header">
              <span class="message-author">${msg.nickname}</span>
              <span class="message-time">${new Date(msg.created_at).toLocaleTimeString()}</span>
            </div>
            <div class="message-text-wrapper">
              <div class="message-text">${this.processMessageContent(msg.content)}</div>
              <div class="chat-reaction-row">
                ${!isOwn ? `
                  <div class="chat-reaction-hover-box">
                    <div class="chat-reaction-option like" onclick="mqttTeam.toggleReaction(${msg.id}, 'LIKE')"><img src="Like.png" alt="like"></div>
                    <div class="chat-reaction-option dislike" onclick="mqttTeam.toggleReaction(${msg.id}, 'DISLIKE')"><img src="Dislike.png" alt="dislike"></div>
                    <div class="chat-reaction-option heart" onclick="mqttTeam.toggleReaction(${msg.id}, 'HEART')"><img src="Heart.png" alt="heart"></div>
                  </div>` : ''}
                ${this.renderReactions(msg)}
              </div>
            </div>
          </div>
        </div>`;
    }).join('');
  }

  processMessageContent(c) {
    return c.replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/@(\w+)/g, `<span class="mention">@$1</span>`);
  }

  renderReactions(msg) {
    if (!msg.reactions || Object.keys(msg.reactions).length === 0) return '';
    return `<div class="chat-reactions">` +
      Object.entries(msg.reactions)
        .filter(([_, users]) => users.length > 0)
        .map(([r, users]) => {
          let icon = r === 'LIKE' ? 'Like.png' : r === 'DISLIKE' ? 'Dislike.png' : 'Heart.png';
          return `<div class="chat-reaction-pill"><img src="${icon}" alt="${r.toLowerCase()}"><span>${users.length}</span></div>`;
        }).join('') +
    `</div>`;
  }

  toggleReaction(messageId, reaction) {
    if (!this.mqttClient?.connected) return;
    this.mqttClient.publish(this.MQTT_TOPICS.REACTION, JSON.stringify({
      messageId, reaction, nickname: this.nickname
    }), { qos: 1 });
  }

  renderUserList() {
    const userList = document.getElementById('userList');
    const userCount = document.getElementById('userCount');
    if (!userList || !userCount) return;
    userCount.textContent = this.activeUsers.size;
    userList.innerHTML = Array.from(this.activeUsers).map(u =>
      `<div class="user-item ${u === this.nickname ? 'current' : ''}">
         <div class="user-name">${u}</div>
       </div>`
    ).join('');
  }

  updateConnectionStatus() {
    const statusText = document.querySelector('#connectionStatus span');
    if (statusText) {
      statusText.textContent = this.isConnected
        ? `연결됨 • ${this.activeUsers.size}명 온라인`
        : '연결 끊김';
    }
  }

  handleLeave() {
    try { if (this.mqttClient?.connected) this.mqttClient.end(); }
    finally { this.showLoginInterface(); }
  }

  showLoginInterface() {
    document.getElementById('chatContainer').style.display = 'none';
    document.getElementById('loginContainer').style.display = 'flex';
    this.nickname = '';
    this.isConnected = false;
    this.mqttClient = null;
    this.messages = [];
  }
}

let mqttTeam;
document.addEventListener('DOMContentLoaded', () => {
  mqttTeam = new MQTTeam();
});
