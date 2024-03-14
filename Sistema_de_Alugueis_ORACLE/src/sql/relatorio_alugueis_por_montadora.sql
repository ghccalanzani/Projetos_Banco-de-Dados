with sumariza_alugueis as (
    select cnpj
         , count(1) as qtd_alugueis
      from alugueis
      group by cnpj
)

select nvl(m.nome_fantasia, m.razao_social) as empresa
     , sa.qtd_alugueis
     , sum(i.valor_aluguel_veiculo) as receita_diaria
     , sum(i.quantidade * i.valor_aluguel_veiculo) as receita_possivel_diaria
  from alugueis a
  inner join sumariza_alugueis sa
  on a.cnpj = sa.cnpj
  inner join montadoras m
  on a.cnpj = m.cnpj
  inner join itens_aluguel i
  on a.codigo_aluguel = i.codigo_aluguel
  group by sa.qtd_alugueis, nvl(m.nome_fantasia, m.razao_social)
