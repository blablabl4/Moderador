# Guia Completo: IDSR Bot Setup

## 🚨 Situação Atual
Detectamos que **Python não está instalado** no seu sistema. Você tem duas opções:

---

## 🎯 OPÇÃO 1: Deploy Direto no Railway (RECOMENDADO)
**Vantagem:** Não precisa instalar nada localmente. O Railway cuida de tudo.

### Passo a Passo:
1. **Instale o Git** (se ainda não tiver):
   - Baixe: https://git-scm.com/download/win
   - Instale com as opções padrão

2. **Crie um repositório no GitHub:**
   - Vá em https://github.com/new
   - Nome: `idsr-bot-moderador` (ou qualquer nome)
   - Deixe **PUBLIC** ou **PRIVATE**
   - **NÃO** adicione README, .gitignore, etc
   - Clique em "Create repository"

3. **Suba o código para o GitHub:**
   Abra o terminal na pasta `c:\Users\Isaque\Desktop\Moderador de grupo` e rode:
   ```bash
   git remote add origin https://github.com/SEU_USUARIO/idsr-bot-moderador.git
   git branch -M main
   git push -u origin main
   ```
   (Substitua `SEU_USUARIO` pelo seu usuário do GitHub)

4. **Deploy no Railway:**
   - Acesse: https://railway.app
   - Clique em "New Project" → "Deploy from GitHub repo"
   - Selecione o repositório `idsr-bot-moderador`
   - O Railway vai detectar o `docker-compose.yml` automaticamente
   - Aguarde os serviços `wppserver` e `bot` serem criados

5. **Configure os Volumes (CRÍTICO!):**
   
   **Serviço `wppserver`:**
   - Clique no serviço `wppserver`
   - Vá na aba "Volumes"
   - Adicione volume: `/usr/src/wpp-server/tokens`
   - Adicione volume: `/usr/src/wpp-server/userData`
   
   **Serviço `bot`:**
   - Clique no serviço `bot`
   - Vá na aba "Volumes"
   - Adicione volume: `/data`

6. **Inicie a Sessão do WhatsApp:**
   - Vá nos logs do serviço `wppserver`
   - Procure pelo QR Code no terminal
   - Escaneie com o WhatsApp da empresa IDSR
   - Pronto! O bot está rodando 🎉

---

## 🖥️ OPÇÃO 2: Rodar Localmente (Teste)
**Vantagem:** Testar antes de fazer o deploy.

### Passo a Passo:
1. **Instale o Python:**
   - Baixe: https://www.python.org/downloads/
   - **IMPORTANTE:** Durante a instalação, marque a caixa "Add Python to PATH"
   - Versão recomendada: 3.10 ou superior

2. **Instale o Docker Desktop** (se quiser rodar com Docker localmente):
   - Baixe: https://www.docker.com/products/docker-desktop
   - Reinicie o computador após a instalação

3. **Abra um NOVO terminal** (após instalar Python) e rode:
   ```bash
   cd "c:\Users\Isaque\Desktop\Moderador de grupo"
   pip install -r requirements.txt
   ```

4. **Rode o bot:**
   ```bash
   python bot.py
   ```
   
   **Nota:** Localmente, sem o WPPConnect server rodando, você verá erros de conexão. Para testar completo, use Docker:
   ```bash
   docker-compose up --build
   ```

---

## 🤝 Minha Recomendação
Vá direto para a **OPÇÃO 1 (Railway)**. É mais simples e é onde o bot vai rodar em produção de qualquer forma.

## 📋 Checklist Final
- [ ] Git instalado
- [ ] Repositório criado no GitHub
- [ ] Código enviado para o GitHub (`git push`)
- [ ] Projeto criado no Railway
- [ ] Volumes configurados nos dois serviços
- [ ] QR Code escaneado no WhatsApp
- [ ] Bot funcionando! ✅
