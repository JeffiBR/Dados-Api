const axios = require('axios');
const fs = require('fs').promises;
const fssync = require('fs');
const path = require('path');
const crypto = require('crypto');
const pLimit = require('p-limit').default;
const winston = require('winston');
require('dotenv').config();

const BASE_DADOS = path.join(__dirname, "dados");
const ARQUIVO_INDICE = path.join(BASE_DADOS, 'index.json');
const LOCK_FILE = path.join(BASE_DADOS, '.lock');
const BACKUP_DIR = path.join(BASE_DADOS, "backup");

// Tempo limite do lock em milissegundos (exemplo: 2 horas)
const LOCK_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 horas

const ECONOMIZA_ALAGOAS_TOKEN = process.env.ECONOMIZA_ALAGOAS_TOKEN || 'token_fake';
const ECONOMIZA_ALAGOAS_API_URL = 'http://api.sefaz.al.gov.br/sfz-economiza-alagoas-api/api/public/produto/pesquisa';

const MERCADOS = [
  { nome: 'Popular Atacarejo', cnpj: '07771407000161', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Jomarte Atacarejo', cnpj: '13152804000158', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Azul Atacarejo', cnpj: '29457887000204', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Bella Compora Rua São João', cnpj: '07671615000431', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Bella Compora Rua Do Sol', cnpj: '07671615000350', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'ATACADAO S.A', cnpj: '75315333014835', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'FELIX SUPERMERCADO', cnpj: '60590998000153', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADO MASTER', cnpj: '01635096000127', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADOS SÃO LUIZ Baixão', cnpj: '15353706000104', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADOS SAO LUIZ Ceci Cunha', cnpj: '15353706000619', categoria: 'Atacarejo', cidade: 'Arapiraca' }
];

const NOMES_PRODUTOS = [
  // Alimentos básicos
  'arroz', 'feijão', 'açúcar', 'óleo', 'café', 'macarrão', 'leite', 'pão', 'farinha', 'sal',
  'arroz integral', 'arroz parboilizado', 'feijão preto', 'feijão carioca', 'lentilha', 'ervilha', 'milho verde',
  'batata', 'batata doce', 'mandioca', 'aipim', 'abóbora', 'cenoura', 'beterraba', 'cebola', 'alho', 'tomate',
  'pepino', 'pimentão', 'abobrinha', 'alface', 'rúcula', 'espinafre',

  // Frutas
  'banana', 'maçã', 'laranja', 'limão', 'uva', 'mamão', 'abacaxi', 'melancia', 'melão', 'pera', 'manga',
  'goiaba', 'kiwi', 'caqui', 'ameixa', 'coco',

  // Carnes bovinas
  'carne', 'carne bovina', 'alcatra', 'contrafilé', 'coxão mole', 'coxão duro', 'patinho', 'maminha', 'fraldinha',
  'filé mignon', 'picanha', 'costela', 'músculo', 'lagarto', 'acém', 'paleta', 'cupim', 'aba de filé', 'bisteca bovina',
  'miolo da paleta', 'ossobuco', 'rabo bovino', 'fígado bovino', 'moela bovina', 'linguiça de boi', 'bife', 'carne moída',
  'rabada', 'vazio', 'matambre', 'peito bovino', 'capa de filé',

  // Carnes suínas
  'carne suína', 'lombo suíno', 'costelinha suína', 'pernil', 'paleta suína', 'bisteca suína', 'linguiça de porco',
  'panceta', 'torresmo', 'joelho de porco', 'pé de porco', 'orelha de porco', 'filé mignon suíno', 'copa lombo', 'costela suína',
  'linguiça calabresa', 'linguiça toscana',

  // Carnes de frango e aves
  'frango', 'peito de frango', 'coxa de frango', 'sobrecoxa', 'asa de frango', 'frango inteiro', 'filezinho sassami',
  'moela de frango', 'fígado de frango', 'coração de frango', 'pescoço de frango', 'pé de frango', 'galinha caipira',
  'peru', 'chester', 'pato', 'codorna', 'frango defumado',

  // Carnes de cordeiro/caprino
  'carne de cordeiro', 'costela de cordeiro', 'paleta de cordeiro', 'pernil de cordeiro', 'carneiro',
  'cabrito', 'paleta de cabrito', 'pernil de cabrito', 'costela de carneiro',

  // Peixes e frutos do mar
  'peixe', 'filé de peixe', 'tilápia', 'salmão', 'bacalhau', 'atum', 'sardinha', 'merluza', 'corvina', 'pescada', 'truta',
  'pirarucu', 'pintado', 'cação', 'anchova', 'dourado', 'tambaqui', 'camarão', 'lula', 'polvo', 'ostra', 'marisco',
  'mexilhão', 'caranguejo', 'lagosta', 'sirigado', 'arraia', 'surubim', 'bacalhau dessalgado', 'bacalhau salgado',

  // Embutidos e defumados
  'presunto', 'mortadela', 'salame', 'linguiça', 'salsicha', 'paio', 'blanquet', 'peito de peru', 'salaminho', 'copa',
  'presunto parma', 'presunto cru', 'pastrami', 'apresuntado', 'fiambre', 'defumado', 'pancetta',

  // Ovos e derivados
  'ovos', 'clara de ovo', 'gema de ovo',

  // Laticínios
  'manteiga', 'margarina', 'iogurte', 'creme de leite', 'leite condensado', 'requeijão', 'queijo minas', 'queijo prato',
  'queijo mussarela',

  // Padaria e confeitaria
  'bolo', 'massa para bolo', 'fermento', 'gelatina', 'sucrilhos', 'aveia', 'granola', 'biscoito', 'bolacha',
  'biscoito recheado', 'biscoito cream cracker', 'biscoito água e sal', 'biscoito de polvilho', 'pão de forma', 'torrada',

  // Doces, chocolates e snacks
  'chocolate', 'doce', 'balas', 'pirulito', 'pipoca', 'bala', 'barrinha de cereal', 'paçoca', 'amendoim', 'castanha',
  'nozes', 'uva passa',

  // Bebidas
  'refrigerante', 'suco', 'água mineral', 'cerveja', 'vinho', 'vodka', 'whisky', 'cachaça', 'energético', 'chá', 'mate',
  'isotônico',

  // Conservas e molhos
  'extrato de tomate', 'molho de tomate', 'maionese', 'ketchup', 'mostarda', 'azeite', 'vinagre', 'azeitona',
  'ervilha em lata', 'milho em lata', 'sardinha em lata', 'atum em lata',

  // Produtos de higiene e limpeza
  'sabão em pó', 'sabão em barra', 'detergente', 'amaciante', 'desinfetante', 'água sanitária', 'multiuso', 'esponja',
  'papel higiênico', 'guardanapo', 'toalha de papel', 'shampoo', 'condicionador', 'sabonete', 'creme dental',
  'escova de dente', 'desodorante', 'absorvente', 'fralda', 'papel toalha', 'alvejante', 'limpa vidro', 'lustra móveis',

  // Animais de estimação
  'ração', 'areia para gato', 'biscoito para cachorro',

  // Outros ingredientes
  'farinha de mandioca', 'farinha de trigo', 'fubá', 'polvilho', 'massa para pastel'
];

const DIAS_PESQUISA = 5;
const REGISTROS_POR_PAGINA = 50;
const INTERVALO_EM_HORAS = 24; // Atualização agora a cada 24 horas
const LIMITE_DIAS_HISTORICO = 35;
const VERSAO_SCRIPT = "2.2.0-pastas-individuais";
const CONCORRENCIA_PRODUTOS = 10;
const CONCORRENCIA_MERCADOS = 5;
const RETRY_MAX = 3;
const RETRY_BASE_MS = 2000;
const BLACKLIST_RETRIES = 2;
const BLACKLIST_TEMPO_MS = 1000 * 60 * 30;

// NOVA FUNÇÃO: Gera pasta específica para cada supermercado
function getMarketFolder(market, dt = new Date()) {
  const yyyy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const dd = String(dt.getDate()).padStart(2, '0');
  const h = String(dt.getHours()).padStart(2, '0');
  const min = String(dt.getMinutes()).padStart(2, '0');
  
  // Normaliza o nome do supermercado para usar como nome de pasta
  const nomeNormalizado = market.nome
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
  
  const folder = path.join(BASE_DADOS, 'supermercados', nomeNormalizado, `${yyyy}-${mm}-${dd}`, `${h}${min}`);
  return folder;
}

// MODIFICADA: Função para pasta de dados geral (para índice e logs)
function getDataFolder(dt = new Date()) {
  const yyyy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const dd = String(dt.getDate()).padStart(2, '0');
  const h = String(dt.getHours()).padStart(2, '0');
  const min = String(dt.getMinutes()).padStart(2, '0');
  const folder = path.join(BASE_DADOS, `${yyyy}-${mm}-${dd}`, `${h}${min}`);
  return folder;
}

// MODIFICADA: Nome do arquivo agora inclui informações do mercado
function getFileName(market, dt = new Date()) {
  const nome = market.nome.replace(/[^\w\d]/g, '_');
  return `dados_${nome}_${market.cnpj}.json`;
}

// NOVA FUNÇÃO: Arquivo de checkpoint específico por supermercado
function getCheckpointFile(market, dt = new Date()) {
  return path.join(getMarketFolder(market, dt), 'checkpoint.json');
}

// NOVA FUNÇÃO: Arquivo de falhas específico por supermercado
function getFailuresFile(market, dt = new Date()) {
  return path.join(getMarketFolder(market, dt), 'failures.json');
}

// NOVA FUNÇÃO: Arquivo de histórico específico por supermercado
function getHistoryFile(market, dt = new Date()) {
  return path.join(getMarketFolder(market, dt), getFileName(market, dt));
}

let blacklist = {};
let failcount = {};

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(info => `[${info.timestamp}] ${info.level.toUpperCase()}: ${info.message}`)
  ),
  transports: [
    new winston.transports.File({ filename: path.join(BASE_DADOS, 'error.log'), level: 'error' }),
    new winston.transports.File({ filename: path.join(BASE_DADOS, 'combined.log') }),
    new winston.transports.Console({ format: winston.format.simple() }),
  ],
});

function normalizarTexto(txt) {
  if (!txt) return "";
  return txt
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function gerarIdProduto(produto, unidade, codBarras) {
  if (codBarras && codBarras.length >= 8) return codBarras;
  return normalizarTexto(produto + "_" + (unidade || "")).replace(/\s/g, "_");
}

function gerarIdRegistro(obj) {
  const hash = crypto.createHash('sha1');
  hash.update([
    obj.cnpj_supermercado,
    obj.id_produto,
    obj.preco_produto,
    obj.data_ultima_venda,
    obj.data_coleta
  ].join('|'));
  return hash.digest('hex').substring(0, 16);
}

function isRegistroIgual(a, b) {
  return a.id_registro === b.id_registro;
}

function filtrarRegistrosRecentes(historico) {
  const agora = new Date();
  return historico.filter(registro => {
    if (!registro.data_coleta) return false;
    const dataColeta = new Date(registro.data_coleta);
    const diffDias = (agora - dataColeta) / (1000 * 60 * 60 * 24);
    return diffDias <= LIMITE_DIAS_HISTORICO;
  });
}

// MODIFICADA: Atualizar índice com informações de pasta do supermercado
async function atualizarIndice(market, ultimaAtualizacao, pastaSupermercado) {
  let indice = {};
  try {
    indice = JSON.parse(await fs.readFile(ARQUIVO_INDICE, 'utf8'));
  } catch (_) {}
  
  indice[market.cnpj] = {
    nome: market.nome,
    categoria: market.categoria || "",
    cidade: market.cidade || "",
    cnpj: market.cnpj,
    ultima_coleta: ultimaAtualizacao,
    pasta_dados: pastaSupermercado // Nova informação sobre onde estão os dados
  };
  
  await fs.mkdir(path.dirname(ARQUIVO_INDICE), { recursive: true });
  await fs.writeFile(ARQUIVO_INDICE, JSON.stringify(indice, null, 2), 'utf8');
}

function validaRegistro(obj) {
  return obj && obj.cnpj_supermercado && obj.id_produto && typeof obj.preco_produto !== 'undefined';
}

function isBlacklisted(cnpj, produto) {
  const key = `${cnpj}|${produto}`;
  const now = Date.now();
  if (blacklist[key] && now < blacklist[key]) return true;
  return false;
}

function registerFailure(cnpj, produto) {
  const key = `${cnpj}|${produto}`;
  failcount[key] = (failcount[key] || 0) + 1;
  if (failcount[key] >= BLACKLIST_RETRIES) {
    blacklist[key] = Date.now() + BLACKLIST_TEMPO_MS;
    logger.warn(`Blacklist temporária: ${key}`);
    failcount[key] = 0;
  }
}

function clearFailure(cnpj, produto) {
  const key = `${cnpj}|${produto}`;
  failcount[key] = 0;
  blacklist[key] = 0;
}

// MODIFICADA: Carregar falhas específicas do supermercado
async function loadFailures(market, dt) {
  try {
    const fails = JSON.parse(await fs.readFile(getFailuresFile(market, dt), 'utf8'));
    blacklist = { ...blacklist, ...(fails.blacklist || {}) };
    failcount = { ...failcount, ...(fails.failcount || {}) };
  } catch (_) {}
}

// MODIFICADA: Salvar falhas específicas do supermercado
async function saveFailures(market, dt) {
  const filePath = getFailuresFile(market, dt);
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify({ blacklist, failcount }, null, 2));
}

// NOVA FUNÇÃO DE LOCK COM CHECAGEM DE LOCK ZUMBI
async function lockExec() {
  if (fssync.existsSync(LOCK_FILE)) {
    try {
      // Lê o conteúdo (timestamp) do arquivo de lock
      const ts = parseInt(await fs.readFile(LOCK_FILE, 'utf-8'));
      const now = Date.now();
      if (isNaN(ts) || now - ts > LOCK_TIMEOUT_MS) {
        // Lock ficou velho: avisar e remover
        console.warn(`[LOCK] Lock antigo detectado (criado em ${new Date(ts).toLocaleString()})! Apagando lock zumbi...`);
        await fs.unlink(LOCK_FILE);
      } else {
        // Lock válido: rejeita execução
        throw new Error(`Já existe execução em andamento (lock criado às ${new Date(ts).toLocaleString()})`);
      }
    } catch (e) {
      throw new Error(`Erro ao checar/remover lock: ${e.message}`);
    }
  }
  // Cria novo lock
  await fs.mkdir(path.dirname(LOCK_FILE), { recursive: true });
  await fs.writeFile(LOCK_FILE, String(Date.now()));
}

async function unlockExec() {
  if (fssync.existsSync(LOCK_FILE)) await fs.unlink(LOCK_FILE);
}

// MODIFICADA: Backup agora considera a estrutura de pastas por supermercado
async function backupData() {
  try {
    await fs.mkdir(BACKUP_DIR, { recursive: true });
    const now = new Date().toISOString().slice(0, 10);
    
    // Backup da pasta de supermercados
    const supermercadosPath = path.join(BASE_DADOS, 'supermercados');
    if (fssync.existsSync(supermercadosPath)) {
      const mercados = await fs.readdir(supermercadosPath);
      for (const mercado of mercados) {
        const mercadoPath = path.join(supermercadosPath, mercado);
        if (fssync.statSync(mercadoPath).isDirectory()) {
          const dates = await fs.readdir(mercadoPath);
          for (const date of dates) {
            const datePath = path.join(mercadoPath, date);
            if (fssync.statSync(datePath).isDirectory()) {
              const times = await fs.readdir(datePath);
              for (const time of times) {
                const timePath = path.join(datePath, time);
                if (fssync.statSync(timePath).isDirectory()) {
                  const files = await fs.readdir(timePath);
                  for (const file of files) {
                    if (file.endsWith('.json')) {
                      const content = await fs.readFile(path.join(timePath, file));
                      const backupFolder = path.join(BACKUP_DIR, 'supermercados', mercado, date, time);
                      await fs.mkdir(backupFolder, { recursive: true });
                      await fs.writeFile(path.join(backupFolder, `${file}.${now}.bak`), content);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    // Backup dos arquivos gerais
    const folders = await fs.readdir(BASE_DADOS);
    for (const folder of folders) {
      if (folder === 'supermercados' || folder === 'backup') continue; // Já processados ou é o próprio backup
      
      const fullFolder = path.join(BASE_DADOS, folder);
      if (fssync.existsSync(fullFolder) && fssync.statSync(fullFolder).isDirectory()) {
        const files = await fs.readdir(fullFolder);
        for (const file of files) {
          if (file.endsWith('.json')) {
            const content = await fs.readFile(path.join(fullFolder, file));
            const backupFolder = path.join(BACKUP_DIR, folder);
            await fs.mkdir(backupFolder, { recursive: true });
            await fs.writeFile(path.join(backupFolder, `${file}.${now}.bak`), content);
          }
        }
      }
    }
    
    logger.info('Backup diário realizado com nova estrutura de pastas.');
  } catch (err) {
    logger.error('Erro no backup: ' + err);
  }
}

// MODIFICADA: Carregar checkpoint específico do supermercado
async function loadCheckpoint(market, dt) {
  try {
    return JSON.parse(await fs.readFile(getCheckpointFile(market, dt), 'utf8'));
  } catch (_) { return {}; }
}

// MODIFICADA: Salvar checkpoint específico do supermercado
async function saveCheckpoint(checkpoint, market, dt) {
  const filePath = getCheckpointFile(market, dt);
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(checkpoint, null, 2));
}

async function retryAsync(fn, cnpj, produto, maxRetries = RETRY_MAX, baseDelay = RETRY_BASE_MS) {
  let attempt = 0;
  let lastErr;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      lastErr = err;
      registerFailure(cnpj, produto);
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
        logger.warn(`Tentativa ${attempt} para ${cnpj}|${produto} falhou. Retry em ${delay}ms`);
        await new Promise(res => setTimeout(res, delay));
      }
    }
  }
  throw lastErr;
}

// MODIFICADA: Função de consulta agora usa estrutura específica por supermercado
async function consultarProduto(produto, mercado, historico, data_coleta, novosRegistrosRef, checkpointRef, dtFolder) {
  const { nome, cnpj, categoria, cidade } = mercado;
  if (isBlacklisted(cnpj, produto)) {
    logger.warn(`Pulando produto em blacklist: ${cnpj}|${produto}`);
    return;
  }
  
  let pagina = checkpointRef[produto] || 1, totalPaginas = 1, sucessoAlguma = false;
  
  do {
    const requestBody = {
      produto: { descricao: produto.toUpperCase() },
      estabelecimento: { individual: { cnpj } },
      dias: DIAS_PESQUISA,
      pagina,
      registrosPorPagina: REGISTROS_POR_PAGINA
    };
    
    const fetchWithRetry = () => axios.post(ECONOMIZA_ALAGOAS_API_URL, requestBody, {
      headers: {
        'AppToken': ECONOMIZA_ALAGOAS_TOKEN,
        'Content-Type': 'application/json'
      },
      timeout: 50000
    });

    let response;
    try {
      response = await retryAsync(fetchWithRetry, cnpj, produto, RETRY_MAX, RETRY_BASE_MS);
      clearFailure(cnpj, produto);
      sucessoAlguma = true;
      
      const conteudo = Array.isArray(response.data?.conteudo) ? response.data.conteudo : [];
      totalPaginas = response.data?.totalPaginas ?? 1;
      
      const resultadosFormatados = conteudo.map(item => {
        const produtoInfo = item.produto || {};
        const venda = produtoInfo.venda || {};
        const nomeNormalizado = normalizarTexto(produtoInfo.descricao);
        const unidade = produtoInfo.unidadeMedida || '';
        const codBarras = produtoInfo.gtin || '';
        const id_produto = gerarIdProduto(produtoInfo.descricao, unidade, codBarras);

        const registro = {
          nome_supermercado: nome,
          cnpj_supermercado: cnpj,
          categoria_supermercado: categoria || "",
          cidade_supermercado: cidade || "",
          nome_produto: produtoInfo.descricao || '',
          nome_produto_normalizado: nomeNormalizado,
          id_produto,
          categoria_produto: produtoInfo.categoria || "",
          preco_produto: venda.valorVenda || '',
          unidade_medida: unidade,
          data_ultima_venda: venda.dataVenda || '',
          data_coleta,
          codigo_barras: codBarras,
          origem: "Economiza Alagoas API",
          versao_script: VERSAO_SCRIPT
        };
        
        registro.id_registro = gerarIdRegistro({
          cnpj_supermercado: cnpj,
          id_produto,
          preco_produto: venda.valorVenda || '',
          data_ultima_venda: venda.dataVenda || '',
          data_coleta
        });
        
        return registro;
      }).filter(validaRegistro);

      for (const r of resultadosFormatados) {
        const existe = historico.some(h => isRegistroIgual(h, r));
        if (!existe) {
          historico.push(r);
          novosRegistrosRef.count++;
        }
      }
      
      logger.info(`[${new Date().toLocaleString()}] CNPJ ${cnpj} - [${nome}] - Produto "${produto}" - Página ${pagina}/${totalPaginas} coletada. Itens: ${resultadosFormatados.length}`);
      
    } catch (error) {
      logger.error(`[${new Date().toLocaleString()}] Erro ao consultar "${produto}" para ${nome} (CNPJ ${cnpj}), página ${pagina}: ${error.response?.data || error.message}`);
      break;
    }
    
    pagina++;
    checkpointRef[produto] = pagina;
    await saveCheckpoint(checkpointRef, mercado, dtFolder);
    
  } while (pagina <= totalPaginas);
  
  if (!sucessoAlguma) registerFailure(cnpj, produto);
}

// MODIFICADA: Salvar resultados na pasta específica do supermercado
async function salvarResultados(market, historico, dt) {
  const nomeArquivo = getHistoryFile(market, dt);
  await fs.mkdir(path.dirname(nomeArquivo), { recursive: true });
  await fs.writeFile(nomeArquivo, JSON.stringify(historico, null, 2), 'utf8');
  logger.info(`Dados salvos para ${market.nome}: ${nomeArquivo}`);
}

// MODIFICADA: Carregar histórico da pasta específica do supermercado
async function carregarHistorico(market, dt) {
  try {
    const nomeArquivo = getHistoryFile(market, dt);
    const dados = await fs.readFile(nomeArquivo, 'utf8');
    return JSON.parse(dados);
  } catch (_) {
    return [];
  }
}

// MODIFICADA: Função principal agora processa cada supermercado em sua pasta individual
async function coletarDadosMercado(mercado, dt) {
  const pastaSupermercado = getMarketFolder(mercado, dt);
  logger.info(`Iniciando coleta para ${mercado.nome} (${mercado.cnpj}) - Pasta: ${pastaSupermercado}`);
  
  // Carrega dados específicos deste supermercado
  await loadFailures(mercado, dt);
  let checkpoint = await loadCheckpoint(mercado, dt);
  let historico = await carregarHistorico(mercado, dt);
  
  // Filtra registros antigos
  historico = filtrarRegistrosRecentes(historico);
  
  const data_coleta = new Date().toISOString();
  const novosRegistrosRef = { count: 0 };
  
  // Limita concorrência para este supermercado
  const limiteProdutos = pLimit(CONCORRENCIA_PRODUTOS);
  
  const promessasProdutos = NOMES_PRODUTOS.map(produto =>
    limiteProdutos(() => consultarProduto(produto, mercado, historico, data_coleta, novosRegistrosRef, checkpoint, dt))
  );
  
  await Promise.all(promessasProdutos);
  
  // Salva dados na pasta específica do supermercado
  await salvarResultados(mercado, historico, dt);
  await saveFailures(mercado, dt);
  
  // Atualiza índice geral
  await atualizarIndice(mercado, data_coleta, pastaSupermercado);
  
  logger.info(`Coleta finalizada para ${mercado.nome}. Novos registros: ${novosRegistrosRef.count}. Total no histórico: ${historico.length}`);
  
  return {
    mercado: mercado.nome,
    cnpj: mercado.cnpj,
    novos_registros: novosRegistrosRef.count,
    total_historico: historico.length,
    pasta_dados: pastaSupermercado
  };
}

// FUNÇÃO PRINCIPAL MODIFICADA
async function main() {
  try {
    await lockExec();
    logger.info(`Iniciando coleta de dados - Versão ${VERSAO_SCRIPT}`);
    
    const dt = new Date();
    await fs.mkdir(BASE_DADOS, { recursive: true });
    
    // Backup diário
    await backupData();
    
    // Limita concorrência entre supermercados
    const limiteMercados = pLimit(CONCORRENCIA_MERCADOS);
    
    const promessasMercados = MERCADOS.map(mercado =>
      limiteMercados(() => coletarDadosMercado(mercado, dt))
    );
    
    const resultados = await Promise.all(promessasMercados);
    
    // Log final com resumo
    logger.info('=== RESUMO DA COLETA ===');
    let totalNovosRegistros = 0;
    let totalHistorico = 0;
    
    for (const resultado of resultados) {
      totalNovosRegistros += resultado.novos_registros;
      totalHistorico += resultado.total_historico;
      logger.info(`${resultado.mercado} (${resultado.cnpj}): ${resultado.novos_registros} novos registros, ${resultado.total_historico} total. Pasta: ${resultado.pasta_dados}`);
    }
    
    logger.info(`TOTAL GERAL: ${totalNovosRegistros} novos registros, ${totalHistorico} registros no histórico`);
    logger.info('Coleta finalizada com sucesso!');
    
  } catch (error) {
    logger.error('Erro na execução principal: ' + error.message);
    throw error;
  } finally {
    await unlockExec();
  }
}

// Execução
if (require.main === module) {
  main().catch(err => {
    logger.error('Erro fatal: ' + err.message);
    process.exit(1);
  });
}

module.exports = {
  main,
  coletarDadosMercado,
  getMarketFolder,
  getHistoryFile,
  MERCADOS,
  NOMES_PRODUTOS
};

