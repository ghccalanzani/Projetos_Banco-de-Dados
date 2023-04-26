select a.codigo_aluguel
     , a.data_aluguel_inicial
	  , a.data_aluguel_final
     , (a.data_aluguel_final - a.data_aluguel_inicial) as quant_dias
     , ((a.data_aluguel_final - a.data_aluguel_inicial) * valor_aluguel_veiculo) as valor_total
     , c.nome as cliente
     , nvl(m.nome_fantasia, m.razao_social) as empresa
     , i.codigo_item_aluguel as item_aluguel
     , vei.modelo_veiculo as veiculo
     , i.quantidade as quant_veiculos_montadora
     , i.valor_aluguel_veiculo
     
  from alugueis a
  inner join clientes c
  on a.cpf = c.cpf
  inner join montadoras m
  on a.cnpj = m.cnpj
  left join itens_aluguel i
  on a.codigo_aluguel = i.codigo_aluguel
  left join veiculos vei
  on i.codigo_veiculo = vei.codigo_veiculo
  order by c.nome