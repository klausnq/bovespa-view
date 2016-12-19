##### CARREGA BIBLIOTECAS E DADOS ####
library(shiny)
#library(datasets)
library(ggplot2)
library(dygraphs)
library(zoo)
library(xts)
#  passa o caminho do spark para o R
Sys.setenv(SPARK_HOME = "/home/klaus/software/spark")
#  carrega biblioteca do spark
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#inicia sessao do sparkR
sc <- sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
########  Abre arquivo de dados ######## 
texto <- read.text("Serie_historica/")
#### remove cabeçalhos e rodapés dos arquivos ####
txt_dados <- filter(texto, substr(texto$value,1,2) == "01")
########  distribui texto em colunas aplicando o esquema de dados e pegando somente as colunas que intreressam ######## 
sdf_sel <- select(txt_dados, 
                  alias(cast(substr(txt_dados$value,4,11),"integer"), "DATA"), alias(substr(txt_dados$value,12,13), "CODBDI"), alias(substr(txt_dados$value,14,25), "CODNEG"), 
                  alias(substr(txt_dados$value,26,28), "TPMERC"), alias(substr(txt_dados$value,29,40), "NOMRES"), alias(substr(txt_dados$value,41,50), "ESPECI"), 
                  alias(cast(substr(txt_dados$value,58,70),"double")/100, "PREABE"), alias(cast(substr(txt_dados$value,71,83),"double")/100, "PREMAX"), 
                  alias(cast(substr(txt_dados$value,84,96),"double")/100, "PREMIN"), alias(cast(substr(txt_dados$value,97,109),"double")/100,"PREMED"), 
                  alias(cast(substr(txt_dados$value,110,122),"double")/100,"PREULT"), alias(cast(substr(txt_dados$value,149,153),"integer"),"TOTNEG"), 
                  alias(cast(substr(txt_dados$value,154,171),"integer"),"QUATOT"), alias(cast(substr(txt_dados$value,172,189),"double")/100,"VOLTOT"), 
                  alias(cast(substr(txt_dados$value,212,218),"integer"),"FATCOT"), alias(substr(txt_dados$value,232,243),"CODISI"), 
                  alias(substr(txt_dados$value,244,246),"DISMES")
)
########  seleciona apenas açoes comercializadas em lote padrao e no mercado a vista (CODBDI = 02 E TPMERC = 010) ######## 
sdf_dados <- filter(sdf_sel, sdf_sel$CODBDI == "02" & sdf_sel$TPMERC == "010")
######## gera lista com os nomes daas açoes em ordem alfabetica ########
nomes <- dropDuplicates(  select(sdf_dados, sdf_dados$CODNEG, alias( format_string("%s: %s - %s",trim(sdf_dados$CODNEG),trim(sdf_dados$NOMRES),trim(sdf_dados$ESPECI) ), "nome") ), "CODNEG")
createOrReplaceTempView(nomes, "table")
acoes <- sql("SELECT * FROM table ORDER BY CODNEG")

#cria lista de ações
choices <- as.data.frame(acoes)
your_choices <- as.list(choices$nome)
names(your_choices) <- choices$nome

#### SERVER ####
server <- function(input, output){
  
  ## 1 - DADOS DE AÇÃO ESPECÍFICA
  
  #COMPUTA DADOS REATIVOS AUTOMATICAMENTE
  #codigo da açao selecionada
  acao <- reactive({ take(filter(acoes,acoes$nome == input$acao),1)$CODNEG })
  #data inicial
  data_ini <- reactive({ as.integer( substr(input$data_inicial,9,10)) + as.integer( substr(input$data_inicial,6,7))*100 + as.integer( substr(input$data_inicial,1,4))*10000 })
  #data final
  data_fin <- reactive({ as.integer( substr(input$data_final,9,10)) + as.integer( substr(input$data_final,6,7))*100 + as.integer( substr(input$data_final,1,4))*10000 })
  
  #DADOS DEPENDENTES DO CLIQUE
  #cotaçoes da açao selecionada para o periodo escolhido
  dados_acao <- reactive({
    if(input$go == 0) return()
    input$go
    isolate({ filter(sdf_dados, ((sdf_dados$CODNEG) == (acao()) ) & sdf_dados$DATA >= data_ini() & sdf_dados$DATA <= data_fin()  ) })
    })
  #ordena dados_acao pela data
  dados_acao_sql <- reactive({
    if(input$go == 0) return()
    input$go
    isolate({
    createOrReplaceTempView(dados_acao(), "dados_sql")
    sql("SELECT * FROM dados_sql ORDER BY DATA")
    })
  })
  
  #texto descritivo da busca
  txt_desc <- reactive({ paste("Cotações da ação ", acao(), "no período entre ", substr(input$data_inicial,9,10),"/", substr(input$data_inicial,6,7), "/", substr(input$data_inicial,1,4),
                               " e ", substr(input$data_final,9,10),"/", substr(input$data_final,6,7), "/", substr(input$data_final,1,4) ) })
  #cotaçao inicial (double e texto)
  cota_ini <- reactive({  first( dados_acao_sql() )$PREULT })
  data_prim <-reactive({  
    data <- first( dados_acao_sql() )$DATA 
    paste( substr(data,7,8),"/", substr(data,5,6), "/", substr(data,1,4) )})
  txt_ini <- reactive({ paste( "\nCotação inicial (R$): ", cota_ini(), " em ", data_prim() )  })
  #cotaçao minima
  cota_min <- reactive({ collect( select( dados_acao_sql(), min(dados_acao_sql()$PREULT) ) )  })
  data_min <- reactive({
    data <- first( where(dados_acao_sql(), dados_acao_sql()$PREULT == as.double(cota_min()) ) )$DATA
    paste( substr(data,7,8),"/", substr(data,5,6), "/", substr(data,1,4) )})
  txt_min <- reactive({ paste( "Cotação minima (R$): ", cota_min(), " em ", data_min() )  })
  #cotação máxima
  cota_max <- reactive({ collect( select( dados_acao_sql(), max(dados_acao_sql()$PREULT) ) )  })
  data_max <- reactive({
    data <- data <- first( where(dados_acao_sql(), dados_acao_sql()$PREULT == as.double(cota_max()) ) )$DATA
    paste( substr(data,7,8),"/", substr(data,5,6), "/", substr(data,1,4) )})
  txt_max <- reactive({ paste( "Cotação máxima (R$): ", cota_max(), " em ", data_max() )  })
  #ultima cotação (double e texto)
  cota_fin <- reactive({ tail( as.data.frame(dados_acao_sql()), 1 )$PREULT })
  data_ult <-reactive({  
    data <- tail( as.data.frame(dados_acao_sql()), 1 )$DATA 
    paste( substr(data,7,8),"/", substr(data,5,6), "/", substr(data,1,4) )})
  txt_fin <- reactive({ paste("Última cotação (R$): ", round(as.double(cota_fin()),2), " em ", data_ult() ) })
  #rendimento acumolado
  rend <- reactive({ 100*(cota_fin() - cota_ini() )/cota_ini() })
  txt_rend <- reactive({ paste("Rendimento acumulado (%): ", round(rend(),2) ) })
  
  #COMPUTA SAIDAS
  # texto descritivo
  output$texto_graf <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({
      txt_desc() 
    })  
  })
  #grafico de cotaçoes como serie temporal
  output$plot <- renderDygraph({ 
    if(input$go == 0) return()
    input$go
    isolate({
      graph_data <- as.data.frame( select( dados_acao(), cast(dados_acao()$DATA,"string"), dados_acao()$PREULT ) )
      graph_data$DATA <- as.Date( as.character(graph_data$DATA), format('%Y%m%d') )
      dygraph( xts(graph_data, order.by = graph_data$DATA) )
    })
  })
  # cotaçao inicial
  output$inicial <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({ txt_ini() }) 
    })
  #cotação final
  output$final <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({ txt_fin() })
  })
  #cotaçao minima
  output$minimo <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({ txt_min() })
  })
  #cotaçao maxima
  output$maximo <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({ txt_max() })
  })
  #rendimento acumulado
  output$rendimento <- renderText({ 
    if(input$go == 0) return()
    input$go
    isolate({ txt_rend() })
  })
  
  
  ## 2 - MAIS RENTÁVEIS NO PERÍODO
  
  #COMPUTA DADOS REATIVOS AUTOMATICAMENTE
  #data inicial
  data_ini_2 <- reactive({ as.integer( substr(input$data_inicial_2,9,10)) + as.integer( substr(input$data_inicial_2,6,7))*100 + as.integer( substr(input$data_inicial_2,1,4))*10000 })
  #data final
  data_fin_2 <- reactive({ as.integer( substr(input$data_final_2,9,10)) + as.integer( substr(input$data_final_2,6,7))*100 + as.integer( substr(input$data_final_2,1,4))*10000 })
  
  #COMPUTA SAIDAS
  #texto de saída
  output$texto_rent <- renderText({ 
    if(input$go_2 == 0)
      return()
    input$go_2
    isolate({ paste("Ações mais rentáveis no periodo entre ", substr(input$data_inicial_2,9,10),"/", substr(input$data_inicial_2,6,7), "/", substr(input$data_inicial_2,1,4),
                    " e ", substr(input$data_final_2,9,10),"/", substr(input$data_final_2,6,7), "/", substr(input$data_final_2,1,4) ) })
  })

  #tabela exibindo as ações mais rentáveis no período
  output$rentaveis <- renderTable({
    if(input$go_2 == 0)
      return()
    input$go_2
    isolate({
      #seleciona dados no periodo
      dados_periodo <- filter( sdf_dados, sdf_dados$DATA >= data_ini_2() & sdf_dados$DATA <= data_fin_2() )
      #seleciona as mais rentáveis no período
      createOrReplaceTempView(dados_periodo, "tabela")
      dados_periodo_sql <- sql("SELECT CODNEG, NOMRES, FIRST(PREULT) AS PRIMEIRA, LAST(PREULT) AS ULTIMA, 100*(LAST(PREULT)-FIRST(PREULT))/FIRST(PREULT) AS RENDIMENTO FROM tabela GROUP BY CODNEG, NOMRES ORDER BY RENDIMENTO DESC")  
      take(dados_periodo_sql,input$num)
    })
  })
}


#### UI ####
ui <- navbarPage("Bovespa - Dados historicos",
  #aba de ações por período
  tabPanel("Ação por periodo",
  #barra de selecao lateral
  sidebarPanel(
    selectInput("acao", "Selecionar ação:",your_choices,NULL),
    #seleciona datas inicial e final
    dateInput("data_inicial", "Data inicial (dd/mm/aaaa)", value = "01/01/2010", format = "dd/mm/yyyy"),
    dateInput("data_final", "Data final (dd/mm/aaaa)", value = "12/31/2016", format = "dd/mm/yyyy"),
    #botão para execução
    actionButton("go", "Analisa ação") ## IMPLEMENTAR A COMPUTAÇAO PELO CLIQUE!!!
  ),
  #painel principal
  mainPanel(
    h3(textOutput("texto_graf")),
    #plotOutput("plot"),
    dygraphOutput("plot"),
    HTML("<br>"),
    textOutput("inicial"),
    textOutput("minimo"),
    textOutput("maximo"),
    textOutput("final"),
    h3(textOutput("texto_rend")),
    h4(textOutput("rendimento"))
    
  )),
  #aba de ações mais rentáveis
  tabPanel("Mais rentáveis por periodo",
           sidebarPanel(
             dateInput("data_inicial_2", "Data inicial (dd/mm/aaaa)", value = "01/01/2010", format = "dd/mm/yyyy"),
             dateInput("data_final_2", "Data final (dd/mm/aaaa)", value = "12/31/2016", format = "dd/mm/yyyy"),
             numericInput("num", "Número de ações", 1, 100, 1),
             actionButton("go_2", "Seleciona mais rentáveis")
           ),
           mainPanel(
             h3(textOutput("texto_rent")),
             tableOutput("rentaveis")
           )
         )
)

shinyApp(ui = ui, server = server)