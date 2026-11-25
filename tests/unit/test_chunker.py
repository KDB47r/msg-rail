from app.rag.chunker import chunk_text


def test_chunk_text_empty():
    assert chunk_text("") == []


def test_chunk_text_splits_long_chinese_text():
    text = "".join(f"这是第{index}句测试内容。" for index in range(200))

    chunks = chunk_text(text, max_tokens=100, overlap_tokens=10)

    assert len(chunks) > 1
    assert all(chunk.token_count <= 100 for chunk in chunks)
    assert all(chunk.content for chunk in chunks)


def test_chunk_text_splits_long_english_text():
    text = " ".join(["This is a sentence."] * 200)

    chunks = chunk_text(text, max_tokens=50, overlap_tokens=5)

    assert len(chunks) > 1
    assert all(chunk.token_count <= 50 for chunk in chunks)
    assert all(chunk.content for chunk in chunks)
