# BRCache

Um cache é uma área de armazenamento e de acesso rápido onde dados que são frequentemente usados ficam armazenados para evitar o acesso desnecessário a uma área onde o acesso é relativamente mais lento.

O BRCache é um sistema de cache de propósito geral desenvolvido em Java. Ele permite o armazenamento de pares chave e valor. Suporta o armazenamento em memória e disco. Possui suporte transacional. Ele pode ser usado embarcado em uma aplicação, como um framework, ou executado em modo standalone.

O pacote org.brandao.brcache é a sua base. Nele possui a classe Cache com métodos  que permitem armazenar e recuperar dados.

## 1- Obtendo o BRCache.


### 1.1 - Obtendo o pacote.

Os pacotes de liberação estão hospedados no sistema de arquivos da SourceForge em formato ZIP. Cada pacote contém jars, exemplos, código fonte e entre outros. Seu download pode ser feito a partir da url http://sourceforge.net/projects/brcache/files/.

### 1.2 - Repositório de artefatos Maven.

Os artefatos são produzidos sob o groupId org.brandao.

##### brcache
Artefato único, necessário para a manipulação dos dados no cache.

O repositório oficial do BRCache é http://brcache.brandao.org/maven/2.

## 2- Visão geral.

O BRCache é um sistema de cache de propósito geral desenvolvido em Java. Ele permite o armazenamento de pares chave e valor. Podendo definir o tempo máximo de vida e o tempo máximo de ociosidade dos itens. Possui suporte transacional e permite o armazenamento em memória, disco ou outro dispositivo. Ele pode ser usado embarcado em uma aplicação, como um framework, ou executado em modo standalone.

O pacote org.brandao.brcache é a sua base. Nele contém as classes BasicCache, Cache e TXCache. A classe BasicCache provê as operações básicas de um cache, que são put, get, getStream, replace,  putIfAbsent, putStream, replaceStream e putIfAbsentStream. A classe Cache provê além das operações básicas, as operações atômicos putIfAbsemt, remove e replace. A classe TXCache provê todos as operações da classe Cache, mas com suporte transacional.

As operações em um cache podem ser divididas em operações de armazenamento, recuperação e exclusão.

As operações de armazenamento são put, replace e putIfAbsent. As operações de recuperação são get e getStream,  e a operação de exclusão é remove.

Um cache tem três buffers de tamanho limitado e dividido em páginas. Eles são denominados nodes, index e data. O buffer node armazena os nós da árvore das chaves. O index armazena os índices das chaves e o data armazena os valores associados às chaves. Ele também possui uma quantidade limitada de processos que fazem a troca dos dados entre a memória e outro dispositivo, por exemplo, disco. É possível manter mais de um cache por aplicação.

### 2.1- Arquitetura.

Um cache é dividido em três buffers de tamanho limitado. Eles são denominados nodes, index e data. Cada buffer é dividido em páginas, e esse por sua vez é subdividido em blocos.

O buffer node é responsável por armazenar os nós da árvore que constitui as chaves. Seu tamanho de bloco é fixo, 576 bytes. Ele tem que usar no mínimo 9% do total da memória utilizada pelo cache. Não sendo inferior a 2.304 bytes (4x576 bytes). O swap factor deve ficar entre 0.1 e 0.3. Ficando acima ou abaixo do limite, pode deixar o cache lento. A troca entre a memória e outro dispositivo é feito por página.

O buffer index é responsável por armazenar o índice das chaves. Seu tamanho de bloco é fixo, 106 bytes. Ele tem que usar no mínimo 3% do total da memória utilizada pelo cache. Não sendo inferior a 424 bytes (4x106 bytes). O swap factor deve ficar entre 0.1 e 0.3. Ficando acima ou abaixo do limite, pode deixar o cache lento. A troca entre a memória e outro dispositivo é feito por página.

O buffer de dados é responsável por armazenar os valores. Seu tamanho de bloco é variável. Podendo ser configurado. Ele tem que usar no máximo 88% do total da memória utilizada pelo cache. Não sendo inferior a quatro vezes o tamanho do bloco (4xbloco). O swap factor deve ficar entre 0.1 e 0.3. Ficando acima ou abaixo do limite, pode deixar o cache lento. A troca entre a memória e outro dispositivo é feito por página.

### 2.2 - Configuração.

Para que o BRCache possa iniciar, é necessário fornecer metadados de configuração. Esses metadados indicam como o cache deve ser montado. São suportados configurações programáticas e via arquivo de propriedades. A configuração consiste da definição do tamanho dos buffers de nós, índices e dados bem como, a estratégia de troca dos dados dos buffers entre a memória e outros dispositivos.

O arquivo de propriedades é um arquivo texto onde cada linha é a definição de uma propriedade. Tendo que ter o formato <nome>=<valor> onde  nome é o nome da propriedade e valor é o valor da propriedade.

```
#Tamanho do buffer usado para armazenar os nós na memória.
nodes_buffer_size=1m


#Tamanho da página do buffer dos nós.
nodes_page_size=1k


#Fator de swap dos nós.
nodes_swap_factor=0.2
```

As propriedades estão descritas abaixo:

- swapper_thread: Número de threads que irão fazer o swap dos dados entre a memória e o disco.
- data_path: Pasta onde o servidor irá fazer o swap dos dados quando o limite da memória for atingido. O valor padrão é /var/brcache.
- nodes_buffer_size: Tamanho do buffer usado para armazenar os nós na memória. O valor padrão é 16m.
- nodes_page_size: Tamanho da página do buffer dos nós. O valor padrão é 16k.
- nodes_swap_factor: Fator de swap dos nós. O valor padrão é 0.3.
- index_buffer_size: Tamanho do buffer usado para armazenar os índices dos itens na memória. O valor padrão é 2m.
- index_page_size: Tamanho da página do buffer dos índices. O valor padrão é 16k.
- index_swap_factor: Fator de swap dos índices. O valor padrão é 0.3.
- data_buffer_size: Tamanho do buffer usado para armazenar os itens na memória. O valor padrão é 64m.
- data_page_size: Tamanho da página do buffer dos itens. O valor padrão é 16k.
- data_block_size: Tamanho do bloco de dados. O valor padrão é 512b.
- data_swap_factor: Fator de swap dos itens. O valor padrão é 0.3.
- max_size_entry: Tamanho máximo em bytes que um item pode ter para ser armazenado no cache. O valor padrão é 1m.
- max_size_key: Tamanho máximo em bytes que uma chave pode ter. O valor padrão é 48.

A configuração programática é feita com o uso da classe BRCacheConfig. Ela possui métodos que permitem definir a configuração de um cache.

```java
BRCacheConfig brcacheConfig = new BRCacheConfig();

brcacheConfig.setNodesBufferSize(1*1024*1024);
brcacheConfig.setNodesPageSize(1*1024);
brcacheConfig.setNodesSwapFactor(0.2);
```

### 2.3 - Iniciando um cache.

Iniciar um cache é extremamente fácil. Basta passar para o construtor de uma dos tipos de cache os metadados de configuração.

O exemplo abaixo demonstra como iniciar um cache usando um arquivo de propriedades.

```java
File f = new File("brcache.conf");

//Carrega o arquivo de configuração.
Configuration config = new Configuration();
config.load(new FileInputStream(f));

//Carrega a configuração
BRCacheConfig brcacheConfig = new BRCacheConfig(config);

//Cria o cache
BasicCache cache = new BasicCache(brcacheConfig);
```
O exemplo abaixo demonstra como configurar programaticamente um cache.

```java
BRCacheConfig brcacheConfig = new BRCacheConfig();

brcacheConfig.setNodesBufferSize(1*1024*1024);
brcacheConfig.setNodesPageSize(1*1024);
brcacheConfig.setNodesSwapFactor(0.2);

//Cria o cache
BasicCache cache = new BasicCache(brcacheConfig);
```
## 3 – Os tipos de cache.

### 3.1 - O cache básico.

O BasicCache provê as operações básicas de um cache. Ele não oferece suporte ao bloqueio dos itens na execução das operações. Um item pode ao mesmo tempo estar sendo removido e recuperado sem que ocorra um erro. Por não existir nenhum tipo de bloqueio, esse é o cache mais rápido.

```java
public class BasicCache 
	extends StreamCache{


    public TXCache getTXCache(){...}


    public boolean replace(String key, Object value, 
        long timeToLive, long timeToIdle) throws StorageException {...}
	
    public boolean replaceStream(String key, InputStream inputData, 
        long timeToLive, long timeToIdle) throws StorageException{...}
	
    public Object putIfAbsent(String key, Object value, 
        long timeToLive, long timeToIdle) throws StorageException{...}
    
    public InputStream putIfAbsentStream(String key, InputStream inputData,
        long timeToLive, long timeToIdle) throws StorageException{...}
    
    public boolean put(String key, Object value, 
        long timeToLive, long timeToIdle) throws StorageException {...}
    
    public boolean putStream(String key, InputStream inputData, 
        long timeToLive, long timeToIdle) throws StorageException{...}
	
    public Object get(String key) throws RecoverException {...}
	
    public InputStream getStream(String key) throws RecoverException {...}
    
    public boolean remove(String key) throws StorageException{...}


    public BRCacheConfig getConfig() {...}
    
}
```

- getTXCache(): obtém o cache com suporte transacional.
- replace(): substitui o valor associado à chave somente se ele existir.
- replaceStream(): substitui o fluxo de bytes associado à chave somente se ele existir.
- putIfAbsent(): associa o valor à chave somente se a chave não estiver associada a um valor.
- putIfAbsentStream(): associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
- put(): associa o valor à chave.
- putStream(): associa o fluxo de bytes do valor à chave.
- get(): obtém o valor associado à chave.
- getStream(): obtém o fluxo de bytes do valor associado à chave.
- remove(): remove o valor associado à chave.
- getConfig(): obtém a configuração do cache.

#### 3.1.1 - Adicionar itens.

São oferecidos vários métodos para inserir um item no cache. Cada um com sua particularidade. Os métodos put e putStream associam um valor a uma chave, mesmo que ela exista. Os dois métodos têm a mesma função, com a diferença que respectivamente um recebe o valor e o outro o fluxo de bytes do valor.

```java
BasicCache cache = ...;
boolean resultado = cache.put(“chave”, objeto, 0, 0);

if(resultado){
    System.out.println(“o valor foi substituído”);
}
else{
    System.out.println(“o valor foi inserido”);
}
```

Os métodos replace e replaceStream substituem o valor associado à chave somente se ele existir. Os dois métodos têm a mesma função, com a diferença que respectivamente um recebe o valor e o outro o fluxo de bytes do valor.

```java
BasicCache cache = ...;
boolean resultado = cache.replace(“chave”, objeto, 0, 0);

if(resultado){
    System.out.println(“o valor foi substituído”);
}
else{
    System.out.println(“o valor não foi inserido”);
}
```

Os métodos putIfAbsent e putIfAbsentStream associam o valor à chave somente se a chave não estiver associada a um valor. Os dois métodos tem a mesma função, com a diferença que respectivamente um recebe o valor e o outro o fluxo de bytes do valor. Eles tem uma particularidade. Quando existe um valor associado a chave, o mesmo é retornado, mas será lançada uma exceção se ele expirar no momento da sua recuperação.

```java
BasicCache cache = ...;
try{
    Object resultado = cache.putIfAbsent(“chave”, objeto, 0, 0);
    if(resultado == null){
        System.out.println(“o valor foi inserido”);
    }
    else{
        System.out.println(“o valor valor atual foi obtido”);
    }
}
catch(StorageException e){
    if(e.getError() == CacheError.ERROR_1030){
        //o valor atual expirou
    }
    throw e;
}
```
#### 3.1.2 - Recuperar itens.

São oferecidos vários métodos para recuperar um item do cache. Eles são get e getStream. Os dois métodos têm a mesma função, com a diferença que respectivamente um recupera o valor e o outro o fluxo de bytes do valor.

```java
BasicCache cache = ...;
Object resultado = cache.get(“chave”);

if(resultado != null){
    System.out.println(“o valor foi encontrado”);
}
else{
    System.out.println(“o valor não existe”);
}
```

####3.1.3 - Remover itens.

Um item é removido com o método remove. Se o item existir, ele irá retornar true. Se não existir ou estiver expirado ele irá retornar false.

```java
BasicCache cache = ...;
boolean resultado = cache.remove(“chave”);

if(resultado){
    System.out.println(“o valor foi removido”);
}
else{
    System.out.println(“o valor não existe”);
}
```

### 3.2 - O cache.

A classe Cache além de prover as operações básica, ela também provê as operações atômicas putIfAbsent, putIfAbsentStream, replace, repalceStream e remove. Os itens, nela, sofrem bloqueio em todas as operações de inserção e remoção para garantir a atomicidade dos métodos. Sendo assim suas operações são mais lentas em comparação às operações da classe BasicCache.

```java
public class Cache 
	extends BasicCache {
    
    /* métodos da classe BasicCache*/


    public boolean replace(String key, 
        Object oldValue, Object newValue, 
        long timeToLive, long timeToIdle) throws StorageException {...}
	
    public boolean remove(String key, 
        Object value) throws StorageException {...}
	
}
```

- replace(): substitui o valor associado à chave somente se ele for igual a um determinado valor.
- remove(): remove o valor associado à chave somente se ele for igual a um determinado valor.

O método replace somente substitui o item se o valor atual for igual a outro valor.

```java
boolean resultado = cache.replace(“chave”, valorAntigo, valor, 0, 0);

if(resultado){
    System.out.println(“o valor foi substituído”);
}
else{
    System.out.println(“o valor não foi inserido”);
}
```

O método remove somente remove o item se o valor atual for igual a outro valor.

```java
boolean resultado = cache.remove(“chave”, valor);

if(resultado){
    System.out.println(“o valor foi removido”);
}
else{
    System.out.println(“o valor não foi encontrado”);
}
```

### 3.3 - O cache transacional.

A classe TXCache oferece as mesmas operações que Cache, mas com um diferencial. Todas as operações, exceto a operação get, podem ser agrupadas em transações ou tratadas como transações individuais.
Atualmente o nível de isolamento oferecido por ela é READ_COMMITTED. As alterações não ficam visíveis para outras transações até que sejam confirmadas com a execução do commit. As transações concorrentes, que executam o get, obtém o valor antigo porque eles não sofrem bloqueios. Caso seja necessário, é possível bloquear um valor na execução do get, impedindo assim, que outras transações tentem obter o valor. 
Toda operação tem um limite de tempo para que seja executada. Por padrão, esse tempo é de cinco minutos, mas podendo ser configurado. 

```java
public class TXCache 
	extends Cache
	implements Serializable{
    
    /* métodos da classe Cache */
    
    public CacheTransactionManager getTransactionManager(){...}
    
    public long getTransactionTimeout() {...}

    public void setTransactionTimeout(long transactionTimeout) {...}

    public CacheTransaction beginTransaction(){...}

    public Object get(String key, 
        boolean forUpdate) throws RecoverException {...}

}
```

- getTransactionManager(): obtém o gestor transacional.
- getTransactionTimeout(): obtém o tempo limite de uma operação.
- setTransactionTimeout(): define o tempo limite de uma operação.
- beginTransaction(): inicia uma transação.
- get(): obtém o valor associado à chave bloqueando ou não seu acesso as demais transações.

Uma transação é iniciada com a execução do método beginTransaction() e finalizada com o método commit(), para confirmar as operações, ou rollback(), para desfazer as operações. 

O exemplo abaixo demonstra uma transação.

```java
Cache nonTransacionalCache = ...;
TXCache cache = nonTransacionalCache.getTXCache();

CacheTransaction tx = cache.beginTransaction();
try{
   cache.put(“chave_a”, valorA, 0, 0);
   cache.remove(“chave_b”, valorB, 0, 0);
   Object valorC = cache.get(“chave_c”);
   /* interação com valorC */
   cache.commit();
}
catch(Throwable e){
   cache.rollback();
}
```

No exemplo acima, as chaves chave_a e chave_b sofreram bloqueio, mas a chave_c não. As transações concorrentes poderão obter ou alterar a chave_c.

Pode existir casos em que é necessário bloquear uma chave para sua manipulação. O exemplo abaixo demonstra como fazê-lo.

```java
Cache nonTransacionalCache = ...;
TXCache cache = nonTransacionalCache.getTXCache();

CacheTransaction tx = cache.beginTransaction();
try{
    //a chave chave_a foi bloqueada.
    Object valorA = cache.get(“chave_a”, true);
    if(valorA != null && valorA.equals(valorEsperado)){
        cache.remove(valorA);
    }
    cache.commit();
}
catch(Throwable e){
   cache.rollback();
}
```

No exemplo acima, a chave chave_a somente será removida se o valorA for igual ao objeto valorEsperado. Ao executar get, a chave_a fica bloqueada para as demais transações até que ela seja finalizada com um commit ou rollback.
