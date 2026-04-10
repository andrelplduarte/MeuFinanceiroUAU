import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from services.orquestrador_lote_uau import processar_entrada_simples_ou_lote


def _listar_txt(pasta: Path) -> list[str]:
    if not pasta.exists():
        return []
    return sorted(str(p.resolve()) for p in pasta.glob("*.txt") if p.is_file())


def main() -> int:
    base = RAIZ / "tests_rapidos"
    entrada = base / "entrada"
    receber_dir = entrada / "receber"
    recebidos_dir = entrada / "recebidos"
    estoque_dir = entrada / "estoque"
    saida_dir = base / "saida" / datetime.now().strftime("%Y%m%d_%H%M%S")
    saida_dir.mkdir(parents=True, exist_ok=True)

    receber = _listar_txt(receber_dir)
    recebidos = _listar_txt(recebidos_dir)
    estoque = _listar_txt(estoque_dir)

    if not receber or not recebidos:
        print("ERRO: faltam arquivos de teste.")
        print(f"- Receber: {len(receber)} arquivo(s) em {receber_dir}")
        print(f"- Recebidos: {len(recebidos)} arquivo(s) em {recebidos_dir}")
        print("Dica: coloque ao menos 1 TXT em cada pasta e rode novamente.")
        return 2

    modo = "POR_EMPREENDIMENTO" if (len(receber) > 1 or len(recebidos) > 1) else ""
    print("Iniciando validação rápida...")
    print(f"Receber: {len(receber)} | Recebidos: {len(recebidos)} | Estoque: {len(estoque)}")
    print(f"Modo: {modo or 'PAR_UNICO'}")
    print(f"Saída: {saida_dir}")

    saida, tempo = processar_entrada_simples_ou_lote(
        receber,
        recebidos,
        str(saida_dir.resolve()),
        modo,
        caminhos_estoque=estoque or None,
    )

    print("OK: validação rápida concluída.")
    print(f"Tempo: {tempo:.2f}s")
    if isinstance(saida, tuple):
        print(f"Arquivo principal: {saida[0]}")
        print(f"Arquivo base: {saida[1]}")
    else:
        print(f"Arquivo principal: {saida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
