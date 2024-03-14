select i.codigo_aluguel
     , i.codigo_item_aluguel
     , i.codigo_veiculo
     , vei.modelo_veiculo
     , vei.cor_veiculo
     , vei.tipo_combustivel
     , i.quantidade
     , i.valor_aluguel_veiculo
     , (i.quantidade * i.valor_aluguel_veiculo) as receita_possivel_diaria
  from itens_aluguel i
  inner join veiculos vei
  on i.codigo_veiculo = vei.codigo_veiculo
  order by i.codigo_aluguel, vei.modelo_veiculo
