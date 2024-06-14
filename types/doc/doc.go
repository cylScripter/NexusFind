package doc

// TF
// @Description: 统计一段文章的词频
// @param listWords
// @return map[string]float64
func TF(listWords []string) []*KeyWord {
	result := make([]*KeyWord, 0)
	// 用map统计单词出现次数
	docFrequency := make(map[string]float64, 0)
	sumWorlds := 0
	for _, word := range listWords {
		docFrequency[word] += 1
		sumWorlds++
	}
	// 计算TF词频： 关键词 / 所有单词
	var keyWord *KeyWord
	for word := range docFrequency {
		keyWord = &KeyWord{
			Word:   word,
			WordTF: float32(docFrequency[word]) / float32(sumWorlds),
		}
		result = append(result, keyWord)
	}
	return result
}
